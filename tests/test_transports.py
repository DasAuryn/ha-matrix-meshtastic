import asyncio
import base64
import sys
import threading
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from bridge.config import RadioSettings
from bridge.security import ChannelSecurityError
from bridge.transports import MeshTransport, RadioMessage


class MeshTransportTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.key = bytes(range(1, 33))
        self.settings = RadioSettings(
            channel_index=2,
            channel_psk=base64.b64encode(self.key).decode(),
            tcp_host="radio.example",
            tcp_port=4404,
            serial_device="/dev/serial/by-id/test-radio",
            ble_address="AA:BB:CC:DD:EE:FF",
        )
        self.channel = SimpleNamespace(
            index=2, role=2, settings=SimpleNamespace(psk=self.key)
        )
        self.connected = threading.Event()
        self.connected.set()
        self.interface = SimpleNamespace(
            isConnected=self.connected,
            localNode=SimpleNamespace(
                channels=[self.channel],
                requestChannels=MagicMock(),
                waitForConfig=MagicMock(return_value=True),
            ),
            myInfo=SimpleNamespace(my_node_num=0x12345678),
            sendText=MagicMock(),
            close=MagicMock(),
        )
        self.transport = MeshTransport(self.settings)
        self.transport._interface = self.interface
        self.transport._loop = None

    async def test_send_uses_only_selected_verified_channel(self):
        await self.transport.send_text("Hallo", 2)
        self.interface.sendText.assert_called_once_with(
            "Hallo", destinationId="^all", wantAck=False, channelIndex=2
        )

    async def test_key_change_blocks_send_before_write(self):
        self.channel.settings.psk = bytes(range(2, 34))
        with self.assertRaises(ChannelSecurityError):
            await self.transport.send_text("Hallo", 2)
        self.interface.sendText.assert_not_called()

    async def test_public_live_channel_blocks_send_despite_private_cache(self):
        def refresh():
            self.interface.localNode.channels = [
                SimpleNamespace(index=2, role=2, settings=SimpleNamespace(psk=b""))
            ]

        self.interface.localNode.requestChannels.side_effect = refresh
        with self.assertRaises(ChannelSecurityError):
            await self.transport.send_text("Hallo", 2)
        self.interface.localNode.requestChannels.assert_called_once()
        self.interface.localNode.waitForConfig.assert_called_once()
        self.interface.sendText.assert_not_called()

    async def test_refresh_timeout_or_failure_prevents_send(self):
        from bridge.transports import TransportUnavailable

        self.interface.localNode.waitForConfig.return_value = False
        with self.assertRaises(TransportUnavailable):
            await self.transport.send_text("Hallo", 2)
        self.interface.localNode.requestChannels.side_effect = OSError("read failed")
        with self.assertRaises(TransportUnavailable):
            await self.transport.send_text("Hallo", 2)
        self.interface.sendText.assert_not_called()

    async def test_missing_disabled_and_public_channels_are_rejected(self):
        for channels in (
            [],
            [SimpleNamespace(index=2, role=0, settings=SimpleNamespace(psk=self.key))],
            [SimpleNamespace(index=2, role=2, settings=SimpleNamespace(psk=b"\x01"))],
            [SimpleNamespace(index=2, role=2, settings=SimpleNamespace(psk=b""))],
        ):
            self.interface.localNode.channels = channels
            with self.assertRaises(ChannelSecurityError):
                await self.transport.check_channel()

    async def test_other_channel_and_oversize_payload_are_rejected(self):
        for body, channel in (("Hallo", 0), ("", 2), ("😀" * 59, 2)):
            with self.assertRaises(ValueError):
                await self.transport.send_text(body, channel)
        self.interface.sendText.assert_not_called()

    async def test_disconnected_interface_is_safe_to_retry(self):
        from bridge.transports import TransportUnavailable

        self.connected.clear()
        with self.assertRaises(TransportUnavailable):
            await self.transport.send_text("Hallo", 2)
        self.interface.sendText.assert_not_called()

    async def test_exception_during_write_is_not_marked_safe_to_retry(self):
        self.interface.sendText.side_effect = OSError("write failed")
        with self.assertRaises(OSError):
            await self.transport.send_text("Hallo", 2)
        self.interface.sendText.assert_called_once()

    def packet(self, **changes):
        packet = {
            "from": 0xAABBCCDD,
            "to": 0xFFFFFFFF,
            "id": 1234,
            "channel": 2,
            "rxTime": 1234567,
            "decoded": {"text": "Hallo Matrix"},
        }
        packet.update(changes)
        return packet

    async def test_receive_only_private_channel_broadcasts(self):
        self.transport._loop = asyncio.get_running_loop()
        self.transport._on_receive(self.packet(), self.interface)
        message = await asyncio.wait_for(self.transport.receive(), 1)
        self.assertEqual(
            message,
            RadioMessage("aabbccdd:000004d2", "!aabbccdd", "Hallo Matrix", 1234567000),
        )

    async def test_receive_ignores_other_interfaces_channels_own_packets_and_dm(self):
        self.transport._loop = asyncio.get_running_loop()
        self.transport._on_receive(self.packet(), object())
        for packet in (
            self.packet(channel=0),
            self.packet(**{"from": 0x12345678}),
            self.packet(to=0x12345678),
            self.packet(decoded={"text": ""}),
            self.packet(decoded={}),
        ):
            self.transport._on_receive(packet, self.interface)
        await asyncio.sleep(0)
        self.assertTrue(self.transport._messages.empty())

    async def test_receive_surfaces_security_failure_without_body(self):
        self.transport._loop = asyncio.get_running_loop()
        self.channel.settings.psk = b""
        self.transport._on_receive(self.packet(), self.interface)
        with self.assertRaises(ChannelSecurityError):
            await asyncio.wait_for(self.transport.receive(), 1)

    async def test_public_live_channel_blocks_receive_despite_private_cache(self):
        self.transport._loop = asyncio.get_running_loop()
        self.transport._on_receive(self.packet(), self.interface)

        def refresh():
            self.interface.localNode.channels = [
                SimpleNamespace(index=2, role=2, settings=SimpleNamespace(psk=b"\x01"))
            ]

        self.interface.localNode.requestChannels.side_effect = refresh
        with self.assertRaises(ChannelSecurityError):
            await asyncio.wait_for(self.transport.receive(), 1)

    async def test_callback_during_refresh_defers_security_check_until_receive(self):
        self.transport._loop = asyncio.get_running_loop()
        self.interface.localNode.channels = None
        self.transport._on_receive(self.packet(), self.interface)

        def finish():
            self.interface.localNode.channels = [self.channel]
            return True

        self.interface.localNode.waitForConfig.side_effect = finish
        message = await asyncio.wait_for(self.transport.receive(), 1)
        self.assertEqual(message.body, "Hallo Matrix")

    async def test_receive_refresh_timeout_does_not_return_packet(self):
        from bridge.transports import TransportUnavailable

        self.transport._loop = asyncio.get_running_loop()
        self.transport._on_receive(self.packet(), self.interface)
        self.interface.localNode.waitForConfig.return_value = False
        with self.assertRaises(TransportUnavailable):
            await asyncio.wait_for(self.transport.receive(), 1)

    async def test_interfaces_use_documented_constructor_parameters(self):
        for connection, module_name, class_name, expected in (
            (
                "usb",
                "serial_interface",
                "SerialInterface",
                {"devPath": self.settings.serial_device, "timeout": 30},
            ),
            (
                "tcp",
                "tcp_interface",
                "TCPInterface",
                {"hostname": self.settings.tcp_host, "portNumber": 4404, "timeout": 30},
            ),
            (
                "bluetooth",
                "ble_interface",
                "BLEInterface",
                {"address": self.settings.ble_address, "timeout": 30},
            ),
        ):
            constructor = MagicMock(return_value=self.interface)
            module = SimpleNamespace(**{class_name: constructor})
            pub = MagicMock()
            with patch.dict(
                sys.modules,
                {
                    "meshtastic": SimpleNamespace(),
                    f"meshtastic.{module_name}": module,
                    "pubsub": SimpleNamespace(pub=pub),
                },
            ):
                transport = MeshTransport(replace(self.settings, connection=connection))
                await transport.connect()
                constructor.assert_called_once_with(**expected)
                pub.subscribe.assert_called_once_with(
                    transport._on_receive, "meshtastic.receive.text"
                )
                await transport.close()
                pub.unsubscribe.assert_called_once_with(
                    transport._on_receive, "meshtastic.receive.text"
                )


if __name__ == "__main__":
    unittest.main()
