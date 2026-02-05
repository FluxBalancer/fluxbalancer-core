from __future__ import annotations

import aiohttp
from contextlib import asynccontextmanager
from fastapi import FastAPI

from src.modules.routing.bootstrap.container import RoutingModule


@asynccontextmanager
async def lifespan(app: FastAPI, module: RoutingModule):
    app.state.clientSession = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=200, ttl_dns_cache=300),
        timeout=aiohttp.ClientTimeout(total=30),
    )

    # старт фоновой задачи обновления метрик (без глобалок)
    module.updater.start()

    yield

    # TODO: aclose redis

    await app.state.clientSession.close()
    await module.updater.stop()
