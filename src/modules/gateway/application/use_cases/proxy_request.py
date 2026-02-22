import time
from dataclasses import dataclass

from aiohttp import ClientSession
from modules.replication.application.replication_manager import ReplicationManager

from modules.gateway.adapters.inbound.http.brs_parser import BRSParser
from modules.gateway.application.dto.brs import BRSRequest
from modules.observability.application.ports.metrics_repository import (
    MetricsRepository,
)
from modules.routing.application.ports.choose_node_port import ChooseNodePort


@dataclass(slots=True)
class ProxyResult:
    body: bytes
    status: int
    headers: dict[str, str]


class ProxyRequestUseCase:
    """
    Оркестратор HTTP-запроса:
    - парсинг BRS
    - выбор ноды
    - репликация (если нужно)
    - запись latency
    """

    def __init__(
        self,
        choose_node: ChooseNodePort,
        replication_manager: ReplicationManager,
        metrics_repo: MetricsRepository,
        client: ClientSession,
    ):
        self.choose_node = choose_node
        self.replication_manager = replication_manager
        self.metrics_repo = metrics_repo
        self.client = client

    async def execute(self, request) -> ProxyResult:
        brs: BRSRequest = BRSParser.parse(request)

        use_replication = (
            brs.replicate_all
            or brs.replications_count is not None
            or brs.replication_strategy_name is not None
        )

        if use_replication:
            response = await self.replication_manager.execute(request, brs)
            return ProxyResult(
                body=response.body,
                status=response.status_code,
                headers=dict(response.headers),
            )

        node_id, host, port = await self.choose_node.execute(brs)
        target_url = f"http://{host}:{port}{request.url.path}"

        start = time.perf_counter()
        async with self.client.request(
            request.method,
            target_url,
            params=request.query_params,
            headers=dict(request.headers),
            data=await request.body(),
        ) as resp:
            body = await resp.read()
            latency_ms = (time.perf_counter() - start) * 1000

            await self.metrics_repo.add_latency(node_id, latency_ms)

            return ProxyResult(
                body=body,
                status=resp.status,
                headers=dict(resp.headers),
            )
