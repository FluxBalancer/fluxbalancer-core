from dataclasses import dataclass

from src.modules.routing.application.dto.brs import BRSRequest


@dataclass(slots=True)
class ReplicationPolicy:
    """Политика определения количества реплик.

    Args:
        default_replicas: Сколько реплик по умолчанию.
        max_replicas: Верхняя граница.
    """

    default_replicas: int = 1
    max_replicas: int = 5

    def resolve_count(self, brs: BRSRequest, available_nodes: int) -> int:
        """Определяет число реплик по BRS.

        Args:
            brs: DTO BRS запроса.
            available_nodes: Сколько узлов доступно.

        Returns:
            Число реплик r (>=1).
        """
        if available_nodes <= 0:
            return 0

        if brs.replicate_all:
            return min(available_nodes, self.max_replicas)

        if brs.replications_count is None:
            return min(self.default_replicas, available_nodes)

        return min(brs.replications_count, available_nodes, self.max_replicas)
