import time

from aiohttp import ClientSession
from src.modules.routing.adapters.inbound.http.brs_parser import BRSParser
from src.modules.routing.application.dto.brs import BRSRequest
from src.modules.routing.application.ports.inbound.node.choose_node_port import (
    ChooseNodePort,
)
from src.modules.routing.application.ports.outbound.metrics.metrics_repository import (
    MetricsRepository,
)
from src.modules.routing.application.usecase.replication.replication_manager import (
    ReplicationManager,
)
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response, JSONResponse


class ProxyMiddleware(BaseHTTPMiddleware):
    INTERNAL_PATHS = {"/stats", "/docs", "/openapi.json", "/redoc"}

    def __init__(
        self,
        app,
        choose_node: ChooseNodePort,
        metrics_repo: MetricsRepository,
        replication_manager: ReplicationManager,
    ):
        super().__init__(app)
        self.choose_node = choose_node
        self.metrics_repo = metrics_repo
        self.replication_manager = replication_manager

    async def dispatch(self, request: Request, call_next):
        if request.url.path in self.INTERNAL_PATHS:
            return await call_next(request)

        client: ClientSession = request.app.state.clientSession
        brs: BRSRequest = BRSParser.parse(request)

        try:
            # return await self.replication_manager.execute(
            #     request=request,
            #     brs=brs,
            #     client=client,
            # )
            node_id, host, port = await self.choose_node.execute(brs)
        except Exception as e:
            return JSONResponse(
                {"detail": f"choose node failed: {repr(e)}"}, status_code=503
            )

        target_url = f"http://{host}:{port}{request.url.path}"
        headers = dict(request.headers)

        start = time.perf_counter()
        async with client.request(
            request.method,
            target_url,
            params=request.query_params,
            headers=headers,
            data=await request.body(),
        ) as resp:
            body = await resp.read()

            latency_ms = (time.perf_counter() - start) * 1000

            # TODO: add diff with deadline time.
            # latency = realtime - deadline_time
            await self.metrics_repo.add_latency(node_id, latency_ms)

            out_headers = dict(resp.headers)

            return Response(
                content=body,
                status_code=resp.status,
                headers=out_headers,
                media_type=out_headers.get("content-type"),
            )
