from dataclasses import dataclass

from src.modules.routing.application.dto.brs import BRSRequest
from src.modules.routing.application.usecase.node.choose_node import ChooseNodeUseCase
from src.modules.routing.domain.policies.replication_policy import ReplicationPolicy


@dataclass(slots=True)
class ReplicationPlan:
    """План выполнения репликации.

    Содержит список целевых узлов, на которые необходимо
    отправить реплики запроса.

    Attributes:
        targets: Список кортежей вида
            (node_id, host, port).
    """

    targets: list[tuple[str, str, int]]


class ReplicationPlanner:
    """Формирует план репликации запроса.

    Отвечает за:
      - определение количества реплик;
      - выбор целевых узлов;
      - построение структуры ReplicationPlan.

    Не выполняет HTTP-запросы.
    """

    def __init__(
        self,
        chooser: ChooseNodeUseCase,
        policy: ReplicationPolicy,
    ):
        self.chooser = chooser
        self.policy = policy

    async def build(self, brs: BRSRequest) -> ReplicationPlan:
        """Строит план репликации.

        Args:
            brs: DTO запроса BRS с параметрами репликации
                и стратегиями балансировки.

        Returns:
            ReplicationPlan: План с выбранными узлами.

        Raises:
            RuntimeError: Если отсутствуют доступные узлы.
        """
        metrics = await self.chooser.metrics_repo.list_latest()
        if not metrics:
            raise RuntimeError("Нет доступных нод")

        replicas = self.policy.resolve(brs, len(metrics))

        targets = []
        for _ in range(replicas):
            node_id, host, port = await self.chooser.execute(brs)
            targets.append((node_id, host, port))

        return ReplicationPlan(targets=targets)
