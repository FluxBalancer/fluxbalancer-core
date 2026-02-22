from contextlib import asynccontextmanager

import aiohttp
from fastapi import FastAPI

from src.modules.routing.adapters.inbound.grpc.metrics_server import (
    start_grpc_metrics_server,
)
from src.modules.routing.bootstrap.container import RoutingModule


@asynccontextmanager
async def lifespan(app: FastAPI, module: RoutingModule):
    app.state.clientSession = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=1000, ttl_dns_cache=300),
        timeout=aiohttp.ClientTimeout(total=30),
    )
    await module.init_async(app.state.clientSession)

    app.state.grpc_server = await start_grpc_metrics_server(
        repo=module.metrics_repo,
        registry=module.registry,
        port=50051,
    )

    yield

    await module.close_redis_if_exist()
    await app.state.clientSession.close()
    await app.state.grpc_server.stop(grace=2)
