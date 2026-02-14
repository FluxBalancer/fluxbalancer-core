from aiohttp import ClientSession
from src.modules.routing.application.dto.brs import BRSRequest
from src.modules.routing.application.usecase.replication.replication_executor import (
    ReplicationExecutor,
)
from src.modules.routing.application.usecase.replication.replication_planner import (
    ReplicationPlanner,
)
from starlette.requests import Request
from starlette.responses import Response


class ReplicationManager:
    """Оркестратор репликации запроса.

    Координирует:
      1. Формирование плана (ReplicationPlanner);
      2. Выполнение реплик (ReplicationExecutor).

    Не содержит логики выбора узлов или определения
    количества реплик — делегирует соответствующим компонентам.
    """

    def __init__(
        self,
        planner: ReplicationPlanner,
        executor: ReplicationExecutor,
    ):
        self.planner = planner
        self.executor = executor

    async def execute(
        self,
        request: Request,
        brs: BRSRequest,
        client: ClientSession,
    ) -> Response:
        """Выполняет репликацию запроса.

        Args:
            request: Входящий HTTP-запрос.
            brs: DTO параметров балансировки и репликации.
            client: Асинхронный HTTP-клиент.

        Returns:
            Response: Ответ первого завершённого запроса.
        """
        plan = await self.planner.build(brs)

        return await self.executor.execute(
            request=request,
            plan=plan,
            client=client,
        )
