import logging
import traceback

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response, JSONResponse

from modules.gateway.application.use_cases.proxy_request import (
    ProxyRequestUseCase,
    ProxyResult,
)

logger = logging.getLogger("proxy")


class ProxyMiddleware(BaseHTTPMiddleware):
    INTERNAL_PATHS = {"/stats", "/docs", "/openapi.json", "/redoc", "/clear"}

    def __init__(self, app):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
        if request.url.path in self.INTERNAL_PATHS:
            return await call_next(request)

        proxy_use_case: ProxyRequestUseCase | None = getattr(
            request.app.state, "proxy_use_case", None
        )
        if proxy_use_case is None:
            return JSONResponse({"detail": "proxy not ready"}, status_code=503)

        try:
            result: ProxyResult = await proxy_use_case.execute(request)
        except Exception:
            traceback.print_exc()
            logger.exception("proxy failed")
            return JSONResponse(
                {"detail": "proxy failed"},
                status_code=503,
            )

        headers = dict(result.headers or {})
        headers["X-Upstream-Socket"] = result.socket
        return Response(
            content=result.body,
            status_code=result.status,
            headers=headers,
        )
