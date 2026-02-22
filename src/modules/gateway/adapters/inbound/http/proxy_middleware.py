from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response, JSONResponse

from modules.gateway.application.use_cases.proxy_request import ProxyRequestUseCase


class ProxyMiddleware(BaseHTTPMiddleware):
    INTERNAL_PATHS = {"/stats", "/docs", "/openapi.json", "/redoc"}

    def __init__(self, app, proxy_use_case: ProxyRequestUseCase):
        super().__init__(app)
        self.proxy_use_case = proxy_use_case

    async def dispatch(self, request: Request, call_next):
        if request.url.path in self.INTERNAL_PATHS:
            return await call_next(request)

        try:
            result = await self.proxy_use_case.execute(request)
        except Exception as e:
            return JSONResponse(
                {"detail": f"proxy failed: {repr(e)}"},
                status_code=503,
            )

        return Response(
            content=result.body,
            status_code=result.status,
            headers=result.headers,
        )
