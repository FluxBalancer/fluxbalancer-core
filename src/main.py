from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from modules.gateway.adapters.inbound.http.proxy_middleware import ProxyMiddleware
from modules.gateway.adapters.inbound.http.router import ChooseNodeRouter
from src.logging_config import setup_logging
from src.modules.routing.bootstrap.container import RoutingModule
from src.modules.routing.bootstrap.lifespan import lifespan

setup_logging()


def create_app() -> FastAPI:
    module = RoutingModule()

    @asynccontextmanager
    async def app_lifespan(app: FastAPI):
        async with lifespan(app, module):
            yield

    app = FastAPI(lifespan=app_lifespan)

    app.include_router(
        ChooseNodeRouter(
            metrics_agg_repo=module.metrics_agg, metrics_repo=module.metrics_repo
        ).router
    )

    app.add_middleware(ProxyMiddleware)

    return app


if __name__ == "__main__":
    app = create_app()
    uvicorn.run(
        app,
        port=8000,
        loop="asyncio",
    )
