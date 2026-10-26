import asyncio
import logging
import os
import signal
from pathlib import Path

from .runtime import RuntimeController


async def main() -> None:
    os.umask(0o077)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    for name in ("nio", "meshtastic", "paho", "bleak", "bridge.mqtt.protocol"):
        logging.getLogger(name).setLevel(logging.WARNING)
    logging.getLogger("nio.crypto.log").setLevel(logging.ERROR)
    data_dir = Path(os.environ.get("BRIDGE_DATA_DIR", "/data"))
    data_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    data_dir.chmod(0o700)
    try:
        runtime = RuntimeController(data_dir, Path(os.environ.get("BRIDGE_OPTIONS", "/data/options.json")))
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, runtime.stop.set)
        logging.info("Matrix-Messenger startet mit Einrichtung und Verwaltung über Ingress.")
        await runtime.run()
    except Exception as exc:
        logging.error("Bridge beendet (%s).", type(exc).__name__)
        raise SystemExit(1) from None


if __name__ == "__main__":
    asyncio.run(main())
