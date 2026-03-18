import time

from starlette.requests import Request
from starlette.responses import Response

from modules.gateway.application.dto.brs import BRSRequest
from modules.replication.application.ports.outbound.replication_runner import (
    ReplicationRunner,
)
from modules.replication.application.services.replication_planner import (
    ReplicationPlanner,
)
from modules.replication.domain.completion.base import CompletionPolicyInput
from modules.replication.domain.model.execution_result import ExecutionResult
from modules.replication.domain.model.replication_command import ReplicationCommand
from modules.replication.domain.model.replication_plan import ReplicationPlan


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
    ):
        self.planner = planner
        self.runner = executor

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
        plan: ReplicationPlan = await self.planner.build(brs)

        cmd = ReplicationCommand(
            method=request.method,
            path=request.url.path,
            query=request.query_params,
            headers=request.headers,
            body=await request.body(),
        )

        deadline_at: float = time.perf_counter() + (brs.deadline_ms / 1000.0)

        result: ExecutionResult = await self.runner.execute(
            cmd=cmd,
            plan=plan,
            policy_input=CompletionPolicyInput(
                strategy_name=brs.completion_strategy_name, k=brs.completion_k
            ),
            deadline_at=deadline_at,
        )
        sockets = ", ".join([target_socket for target_socket in result.started_nodes])

        return (
            Response(
                content=result.body,
                status_code=result.status,
                headers=result.headers or {},
            ),
            sockets,
        )
