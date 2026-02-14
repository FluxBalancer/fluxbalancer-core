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

        async def call(node_id, host, port):
            start = time.perf_counter()

            async with client.request(
                request.method,
                f"http://{host}:{port}{request.url.path}",
                params=request.query_params,
                headers=dict(request.headers),
                data=await request.body(),
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

        tasks = [
            asyncio.create_task(call(node_id, host, port))
            for node_id, host, port in plan.targets
        ]

        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

        return list(done)[0].result()
