import asyncio
import time

from starlette.requests import Request
from starlette.responses import Response

from modules.gateway.application.dto.brs import BRSRequest
from modules.replication.adapters.outbound.registries.completion_strategy_registry import (
    CompletionStrategyRegistry,
)
from modules.replication.application.ports.outbound.latency_recorder import (
    LatencyRecorder,
)
from modules.replication.application.ports.outbound.replication_runner import (
    ReplicationRunner,
)
from modules.replication.application.services.replication_planner import (
    ReplicationPlanner,
)
from modules.replication.domain.completion import CompletionPolicy
from modules.replication.domain.model.execution_result import ExecutionResult
from modules.replication.domain.model.replication_command import ReplicationCommand


class ReplicationManager:
    """Оркестратор репликации запроса.

    Координирует:
      1. Формирование плана (ReplicationPlanner);
      2. Выполнение реплик (ReplicationRunner).

    Не содержит логики выбора узлов или определения
    количества реплик — делегирует соответствующим компонентам.
    """

    def __init__(
        self,
        planner: ReplicationPlanner,
        executor: ReplicationRunner,
        completion_registry: CompletionStrategyRegistry,
        latency_recorder: LatencyRecorder,
    ):
        self.planner = planner
        self.runner = executor
        self.completion_registry = completion_registry
        self.latency_recorder = latency_recorder

    async def execute(
        self,
        request: Request,
        brs: BRSRequest,
    ) -> Response:
        """Выполняет репликацию запроса.

        Args:
            request: Входящий HTTP-запрос.
            brs: DTO параметров балансировки и репликации.

        Returns:
            Response: Ответ первого завершённого запроса.
        """
        plan = await self.planner.build(brs)

        cmd = ReplicationCommand(
            method=request.method,
            path=request.url.path,
            query=request.query_params,
            headers=request.headers,
            body=await request.body(),
        )

        policy: CompletionPolicy = self.completion_registry.get(
            brs.completion_strategy_name,
            k=brs.completion_k,
        )

        deadline_at: float = time.perf_counter() + (brs.deadline_ms / 1000.0)
        result: ExecutionResult = self.runner.execute(
            cmd=cmd, plan=plan, policy=policy, deadline_at=deadline_at
        )
        await self.latency_recorder.record(result.node_id, result.latency_ms)

        return Response(content=result.body, status_code=result.status)
