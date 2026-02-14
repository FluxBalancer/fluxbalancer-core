import asyncio
import logging
import time

from aiohttp import ClientSession
from src.modules.routing.application.dto.brs import BRSRequest
from src.modules.routing.application.ports.outbound.metrics.metrics_repository import (
    MetricsRepository,
)
from src.modules.routing.application.usecase.node.choose_node import ChooseNodeUseCase
from src.modules.routing.domain.policies.replication_policy import ReplicationPolicy
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger("replication")


class ReplicationManager:
    """
    Hedged requests manager.
    Запускает несколько запросов и возвращает первый успешный.
    """

    def __init__(
            self,
            chooser: ChooseNodeUseCase,
            metrics_repo: MetricsRepository,
            policy: ReplicationPolicy,
    ):
        self.chooser = chooser
        self.metrics_repo = metrics_repo
        self.policy = policy

    async def execute(
            self,
            request: Request,
            brs: BRSRequest,
            client: ClientSession,
    ) -> Response:

        metrics = await self.chooser.metrics_repo.list_latest()
        if not metrics:
            raise RuntimeError("Нет доступных нод")

        replicas = self.policy.resolve(brs, len(metrics))

        # сортируем ноды по приоритету (лучшие вперёд)
        ranked = []
        for _ in range(len(metrics)):
            node_id, host, port = await self.chooser.execute(brs)
            ranked.append((node_id, host, port))

        ranked = ranked[:replicas]

        async def call(node_id, host, port):
            target_url = f"http://{host}:{port}{request.url.path}"

            start = time.perf_counter()

            async with client.request(
                    request.method,
                    target_url,
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
            for node_id, host, port in ranked
        ]

        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )

        # отменяем проигравших
        for task in pending:
            task.cancel()

        result = list(done)[0].result()

        logger.info(
            {
                "message": "replication_winner",
                "replicas": replicas,
            }
        )

        return result
