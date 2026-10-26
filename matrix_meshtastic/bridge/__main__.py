import asyncio
import logging
import os
import signal
from pathlib import Path

from .config import Settings
from .security import ChannelSecurityError
from .service import Bridge


async def main() -> None:
    os.umask(0o077)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    for name in ("nio", "meshtastic", "paho", "bleak", "bridge.mqtt.protocol"):
        logging.getLogger(name).setLevel(logging.WARNING)
    data_dir = Path(os.environ.get("BRIDGE_DATA_DIR", "/data"))
    data_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    data_dir.chmod(0o700)
    try:
        settings = Settings.load(Path(os.environ.get("BRIDGE_OPTIONS", "/data/options.json")))
        bridge = Bridge(settings, data_dir)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, bridge.stop.set)
        logging.info("Matrix-Messenger startet; Meshtastic-Weiterleitung %s.", "aktiviert" if settings.meshtastic.enabled else "deaktiviert")
        await bridge.run()
    except (ChannelSecurityError, ValueError) as exc:
        logging.error("Bridge gesperrt: %s", exc)
        raise SystemExit(1) from None
    except Exception as exc:
        logging.error("Bridge beendet (%s).", type(exc).__name__)
        raise SystemExit(1) from None


if __name__ == "__main__":
    asyncio.run(main())
