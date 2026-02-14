import asyncio
import time

from aiohttp import ClientSession
from src.modules.routing.application.ports.outbound.metrics.metrics_repository import (
    MetricsRepository,
)
from src.modules.routing.application.usecase.replication.replication_planner import (
    ReplicationPlan,
)
from starlette.requests import Request
from starlette.responses import Response


class ReplicationExecutor:
    """Выполняет конкурентные репликации (hedged requests).

    Отвечает исключительно за:
      - конкурентный запуск HTTP-запросов;
      - возврат первого завершившегося результата;
      - отмену остальных задач;
      - запись latency победившего узла.
    """

    def __init__(self, metrics_repo: MetricsRepository):
        self.metrics_repo = metrics_repo

    async def execute(
        self,
        request: Request,
        plan: ReplicationPlan,
        client: ClientSession,
    ) -> Response:
        """Выполняет план репликации.

        Args:
            request: Входящий HTTP-запрос.
            plan: План с целевыми узлами.
            client: Асинхронный HTTP-клиент.

        Returns:
            Response: Ответ первого успешно завершённого запроса.
        """
        body_bytes = await request.body()

        tasks = [
            asyncio.create_task(self._call(node_id, host, port, body_bytes))
            for node_id, host, port in plan.targets
        ]

        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )
        winner = next(iter(done))

        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

        return await winner

    @staticmethod
    async def _call(node_id, host, port, data: bytes):
        start = time.perf_counter()

        try:
            async with client.request(
                    request.method,
                    f"http://{host}:{port}{request.url.path}",
                    params=request.query_params,
                    headers=dict(request.headers),
                    data=data,
            ) as resp:
                body = await resp.read()

                latency = (time.perf_counter() - start) * 1000
                await self.metrics_repo.add_latency(node_id, latency)

                return Response(
                    content=body,
                    status_code=resp.status,
                    headers=dict(resp.headers),
                    media_type=resp.headers.get("content-type"),
                )

        except asyncio.CancelledError:
            # Очень важно — корректно пробросить отмену
            raise