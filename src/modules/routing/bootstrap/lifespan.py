from __future__ import annotations

from contextlib import asynccontextmanager

import aiohttp
from fastapi import FastAPI

from src.modules.routing.adapters.inbound.grpc.metrics_server import start_grpc_metrics_server
from src.modules.routing.bootstrap.container import RoutingModule


@asynccontextmanager
async def lifespan(app: FastAPI, module: RoutingModule):
    app.state.clientSession = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=200, ttl_dns_cache=300),
        timeout=aiohttp.ClientTimeout(total=30),
    )

    # старт фоновой задачи обновления метрик (без глобалок)
    module.updater.start()

    app.state.grpc_server = await start_grpc_metrics_server(
        repo=module.metrics_repo,
        registry=module.registry,
        host="0.0.0.0",
        port=50051,
    )

    yield

    # TODO: aclose redis

    await app.state.clientSession.close()
    await module.updater.stop()

    await app.state.grpc_server.stop(grace=2)
