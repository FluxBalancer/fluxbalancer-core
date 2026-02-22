from starlette.requests import Request
from starlette.responses import Response

from modules.gateway.application.dto.brs import BRSRequest
from modules.replication.adapters.outbound.registries.completion_strategy_registry import (
    CompletionStrategyRegistry,
)
from modules.replication.application.ports.outbound.replication_runner import (
    ReplicationRunner,
)
from modules.replication.application.services.replication_planner import (
    ReplicationPlanner,
)
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
    ):
        self.planner = planner
        self.runner = executor
        self.completion_registry = completion_registry

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

        policy = self.completion_registry.get(
            brs.completion_strategy_name,
            k=brs.completion_k,
        )

        result: ExecutionResult = await self.runner.execute(
            cmd=cmd, plan=plan, policy=policy
        )

        return Response(content=result.body, status_code=result.status)
