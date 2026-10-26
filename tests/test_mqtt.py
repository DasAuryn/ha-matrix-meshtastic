import asyncio
import base64
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import paho.mqtt.client as mqtt
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from meshtastic.protobuf import mesh_pb2, mqtt_pb2, portnums_pb2

from bridge.config import RadioSettings
from bridge.mqtt import BROADCAST, MqttTransport, channel_hash, crypt_payload
from bridge.security import ChannelSecurityError
from bridge.transports import TransportUnavailable


class PayloadCryptoTests(unittest.TestCase):
    def test_aes_matches_nist_cavs_keysbox_128_and_256_at_zero_counter(self):
        for key, expected in (
            ("10a58869d74be5a374cf867cfb473859", "6d251e6944b051e04eaa6fb4dbf78465"),
            (
                "c47b0294dbbbee0fec4757f22ffeee3587ca4730c3d33b691df38bab076bc558",
                "46f2fb342d6f0ab477476fc501242c5f",
            ),
        ):
            self.assertEqual(
                crypt_payload(bytes(16), bytes.fromhex(key), 0, 0),
                bytes.fromhex(expected),
            )

    def test_firmware_nonce_little_endian_layout_and_ctr_block_increment(self):
        key = bytes(range(1, 33))
        nonce = bytes.fromhex("0403020100000000ddccbbaa00000000")
        blocks = b"".join(
            (int.from_bytes(nonce, "big") + offset).to_bytes(16, "big")
            for offset in range(4)
        )
        ecb = Cipher(algorithms.AES(key), modes.ECB()).encryptor()
        keystream = ecb.update(blocks) + ecb.finalize()
        plaintext = bytes(range(57))
        expected = bytes(left ^ right for left, right in zip(plaintext, keystream))
        ciphertext = crypt_payload(plaintext, key, 0xAABBCCDD, 0x01020304)
        self.assertEqual(ciphertext, expected)
        self.assertEqual(
            crypt_payload(ciphertext, key, 0xAABBCCDD, 0x01020304), plaintext
        )

    def test_channel_hash_is_firmware_name_and_key_xor(self):
        self.assertEqual(channel_hash("Test", bytes((1, 2, 3, 4))), 50)


class MqttTransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temporary.name)
        self.key = bytes(range(1, 33))
        self.settings = RadioSettings(
            connection="mqtt",
            channel_index=2,
            channel_psk=base64.b64encode(self.key).decode(),
            mqtt_host="mqtt.example",
            mqtt_topic="msh/EU_868/2/e",
            mqtt_channel_name="Private",
            mqtt_gateway_id="!12345678",
        )
        self.transport = MqttTransport(self.settings, self.data_dir)
        self.transport.loop = asyncio.get_running_loop()
        self.client = MagicMock()
        self.client.is_connected.return_value = True
        self.info = MagicMock(rc=mqtt.MQTT_ERR_SUCCESS)
        self.info.is_published.return_value = True
        self.client.publish.return_value = self.info
        self.transport.client = self.client
        self.transport.connected.set()

    async def asyncTearDown(self):
        await self.transport.close()
        self.temporary.cleanup()

    def message(self, envelope=None, **changes):
        value = SimpleNamespace(
            retain=False,
            topic=self.transport.inbound_topic,
            payload=(envelope or self.envelope()).SerializeToString(),
        )
        for key, item in changes.items():
            setattr(value, key, item)
        return value

    def envelope(self, **changes):
        data = mesh_pb2.Data(
            portnum=portnums_pb2.TEXT_MESSAGE_APP, payload="Grüße aus dem Mesh".encode()
        )
        packet = mesh_pb2.MeshPacket(
            to=BROADCAST,
            id=1234,
            channel=self.transport.hash,
            rx_time=1234567,
            encrypted=crypt_payload(data.SerializeToString(), self.key, 0xAABBCCDD, 1234),
        )
        setattr(packet, "from", 0xAABBCCDD)
        envelope = mqtt_pb2.ServiceEnvelope(
            packet=packet,
            channel_id=self.settings.mqtt_channel_name,
            gateway_id=self.settings.mqtt_gateway_id,
        )
        for key, value in changes.items():
            if key in {"channel_id", "gateway_id"}:
                setattr(envelope, key, value)
            else:
                setattr(envelope.packet, key, value)
        return envelope

    async def test_publish_is_encrypted_protobuf_without_retention(self):
        body = "Matrix-Nachricht mit Umlauten äöü"
        await self.transport.send_text(body, 2)
        args, kwargs = self.client.publish.call_args
        self.assertEqual(args[0], self.transport.outbound_topic)
        self.assertEqual(kwargs, {"qos": 0, "retain": False})
        self.assertNotIn(body.encode(), args[1])
        envelope = mqtt_pb2.ServiceEnvelope.FromString(args[1])
        packet = envelope.packet
        self.assertEqual(packet.WhichOneof("payload_variant"), "encrypted")
        self.assertEqual(envelope.channel_id, "Private")
        self.assertEqual(packet.channel, self.transport.hash)
        self.assertEqual(envelope.gateway_id, self.transport.bridge_id)
        self.assertNotEqual(envelope.gateway_id, self.settings.mqtt_gateway_id)
        self.assertEqual(getattr(packet, "from"), self.transport.sender)
        self.assertNotEqual(getattr(packet, "from"), 0x12345678)
        self.assertEqual(packet.to, BROADCAST)
        data = mesh_pb2.Data.FromString(
            crypt_payload(packet.encrypted, self.key, self.transport.sender, packet.id)
        )
        self.assertEqual(data.portnum, portnums_pb2.TEXT_MESSAGE_APP)
        self.assertEqual(data.payload.decode(), body)
        self.info.wait_for_publish.assert_called_once_with(15)

    async def test_reconnect_stops_old_network_loop_before_replacing_client(self):
        self.transport.connected.clear()
        replacement = MagicMock()
        replacement.loop_start.side_effect = self.transport.connected.set
        with patch("bridge.mqtt.mqtt.Client", return_value=replacement):
            await self.transport.connect()
        self.client.disconnect.assert_called_once()
        self.client.loop_stop.assert_called_once()
        self.assertIs(self.transport.client, replacement)

    async def test_accepts_only_matching_encrypted_broadcast(self):
        self.transport._on_message(self.client, None, self.message())
        message = await asyncio.wait_for(self.transport.receive(), 1)
        self.assertEqual(message.body, "Grüße aus dem Mesh")
        self.assertEqual(message.sender, "!aabbccdd")
        self.assertEqual(message.event_id, "aabbccdd:000004d2")
        self.assertEqual(message.timestamp_ms, 1234567000)

    async def test_rejects_retained_wrong_topic_oversize_and_malformed_messages(self):
        for message in (
            self.message(retain=True),
            self.message(topic=self.transport.outbound_topic),
            self.message(payload=b"bad protobuf"),
            self.message(payload=bytes(4097)),
        ):
            self.transport._on_message(self.client, None, message)
        await asyncio.sleep(0)
        self.assertTrue(self.transport.queue.empty())

    async def test_rejects_other_gateway_channel_dm_own_sender_and_pki(self):
        for envelope in (
            self.envelope(gateway_id="!87654321"),
            self.envelope(channel_id="Public"),
            self.envelope(channel=(self.transport.hash + 1) % 256),
            self.envelope(to=0x12345678),
            self.envelope(**{"from": self.transport.sender}),
            self.envelope(**{"from": 0}),
            self.envelope(id=0),
            self.envelope(pki_encrypted=True),
        ):
            self.transport._on_message(self.client, None, self.message(envelope))
        await asyncio.sleep(0)
        self.assertTrue(self.transport.queue.empty())

    async def test_rejects_plaintext_invalid_key_and_nontext_data(self):
        clear = self.envelope()
        clear.packet.decoded.CopyFrom(
            mesh_pb2.Data(portnum=portnums_pb2.TEXT_MESSAGE_APP, payload=b"plaintext")
        )
        wrong_key = self.envelope()
        wrong_key.packet.encrypted = crypt_payload(
            mesh_pb2.Data(portnum=portnums_pb2.TEXT_MESSAGE_APP, payload=b"wrong key").SerializeToString(),
            bytes(range(2, 34)),
            0xAABBCCDD,
            1234,
        )
        telemetry = self.envelope()
        telemetry.packet.encrypted = crypt_payload(
            mesh_pb2.Data(portnum=portnums_pb2.TELEMETRY_APP, payload=b"telemetry").SerializeToString(),
            self.key,
            0xAABBCCDD,
            1234,
        )
        for envelope in (clear, wrong_key, telemetry):
            self.transport._on_message(self.client, None, self.message(envelope))
        await asyncio.sleep(0)
        self.assertTrue(self.transport.queue.empty())

    async def test_nonce_counter_and_sender_survive_reopening(self):
        await self.transport.send_text("same text", 2)
        first = mqtt_pb2.ServiceEnvelope.FromString(self.client.publish.call_args.args[1])
        identity = self.transport.sender
        await self.transport.close()
        self.transport = MqttTransport(self.settings, self.data_dir)
        self.transport.client = self.client
        self.transport.connected.set()
        await self.transport.send_text("same text", 2)
        second = mqtt_pb2.ServiceEnvelope.FromString(self.client.publish.call_args.args[1])
        self.assertEqual(self.transport.sender, identity)
        self.assertEqual(first.packet.id, 1)
        self.assertEqual(second.packet.id, 2)
        self.assertNotEqual(first.packet.encrypted, second.packet.encrypted)

    async def test_invalid_channel_and_disconnected_broker_cannot_publish(self):
        with self.assertRaises(ChannelSecurityError):
            await self.transport.send_text("Hallo", 0)
        self.transport.connected.clear()
        with self.assertRaises(TransportUnavailable):
            await self.transport.send_text("Hallo", 2)
        self.client.publish.assert_not_called()

    async def test_uncertain_publish_does_not_reuse_nonce(self):
        self.info.is_published.return_value = False
        with self.assertRaises(RuntimeError):
            await self.transport.send_text("Hallo", 2)
        first_id = mqtt_pb2.ServiceEnvelope.FromString(self.client.publish.call_args.args[1]).packet.id
        self.info.is_published.return_value = True
        await self.transport.send_text("Hallo", 2)
        second_id = mqtt_pb2.ServiceEnvelope.FromString(self.client.publish.call_args.args[1]).packet.id
        self.assertEqual(second_id, first_id + 1)

    async def test_exhausted_nonce_counter_fails_closed(self):
        with self.transport.database:
            self.transport.database.execute("UPDATE identity SET next_id = ?", (0x100000000,))
        with self.assertRaises(ChannelSecurityError):
            await self.transport.send_text("Hallo", 2)
        self.client.publish.assert_not_called()


if __name__ == "__main__":
    unittest.main()
