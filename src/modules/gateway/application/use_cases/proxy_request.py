import asyncio
import logging
import time
from dataclasses import dataclass

from aiohttp import ClientSession, ClientTimeout

from modules.gateway.adapters.inbound.http.brs_parser import BRSParser
from modules.gateway.application.dto.brs import BRSRequest
from modules.observability.application.ports.metrics_repository import (
    MetricsRepository,
)
from modules.replication.application.services.replication_manager import (
    ReplicationManager,
)
from modules.routing.application.ports.choose_node_port import ChooseNodePort

logger = logging.getLogger("proxy.use_case")


@dataclass(slots=True)
class ProxyResult:
    socket: str
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
            response, sockets = await self.replication_manager.execute(request, brs)
            return ProxyResult(
                socket=sockets,
                body=response.body,
                status=response.status_code,
                headers=dict(response.headers),
            )

        node_id, host, port = await self.choose_node.execute(brs)
        socket = f"{host}:{port}"

        logger.info("request without replication to %s on node=%s", socket, node_id)

        target_url = f"http://{host}:{port}{request.url.path}"
        start = time.perf_counter()

        timeout = ClientTimeout(total=brs.deadline_ms / 1000)
        try:
            async with self.client.request(
                request.method,
                target_url,
                params=request.query_params,
                headers=dict(request.headers),
                data=await request.body(),
                timeout=timeout,
            ) as resp:
                body = await resp.read()
                await self._record_latency(node_id, start, True)

                return ProxyResult(
                    socket=socket,
                    body=body,
                    status=resp.status,
                    headers=dict(resp.headers),
                )
        except asyncio.TimeoutError:
            await self._record_latency(node_id, start, False)

            return ProxyResult(
                socket=socket,
                body=b"",
                status=504,
                headers={},
            )
        except Exception as e:
            await self._record_latency(node_id, start, False)
            logger.exception("proxy request failed: %s", e)

            return ProxyResult(
                socket=socket,
                body=b"",
                status=500,
                headers={},
            )

    async def _record_latency(self, node_id: str, start: float, success: bool):
        latency_ms = (time.perf_counter() - start) * 1000
        if success:
            latency = latency_ms
        else:
            latency = latency_ms * 1.5
        await self.metrics_repo.add_latency(node_id, latency)
