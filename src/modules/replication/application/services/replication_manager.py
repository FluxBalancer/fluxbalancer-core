from starlette.requests import Request
from starlette.responses import Response

from modules.gateway.application.dto.brs import BRSRequest
from modules.replication.application.ports.outbound.replication_runnter import (
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
        plan = await self.planner.build(brs)

        cmd = ReplicationCommand(
            method=request.method,
            path=request.url.path,
            query=request.query_params,
            headers=request.headers,
            body=await request.body(),
        )
        result: ExecutionResult = await self.runner.execute(cmd, plan)

        return Response(content=result.body, status_code=result.status)
