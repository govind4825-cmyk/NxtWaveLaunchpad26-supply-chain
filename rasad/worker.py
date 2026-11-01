"""Standalone worker: runs every agent and the Traccar link without the API.

Use this when RASAD_EMBED_WORKER=false and the API runs as a separate process.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from .agents.cartographer import Cartographer
from .agents.dispatcher import Dispatcher
from .agents.procurer import Procurer
from .agents.quartermaster import Quartermaster
from .agents.sentinel import Sentinel
from .config import get_settings
from .connectors.boundaries import Boundaries
from .db import close_pool, get_pool, init_schema
from .provenance import Fetcher
from .traccar_link import TraccarLink

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


async def main() -> None:
    s = get_settings()
    await init_schema()
    pool = await get_pool()
    fetcher = Fetcher(pool)
    sentinel = Sentinel(pool, fetcher, Boundaries(s.boundaries_path))
    traccar = TraccarLink(pool)
    cartographer = Cartographer(pool, fetcher)
    quartermaster = Quartermaster(pool, fetcher)
    procurer = Procurer(pool)
    dispatcher = Dispatcher(pool)
    await sentinel.start()
    await cartographer.start()
    await quartermaster.start()
    await procurer.start()
    await dispatcher.start()
    await traccar.start()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()

    await sentinel.stop()
    await cartographer.stop()
    await quartermaster.stop()
    await procurer.stop()
    await dispatcher.stop()
    await traccar.stop()
    await fetcher.aclose()
    await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
