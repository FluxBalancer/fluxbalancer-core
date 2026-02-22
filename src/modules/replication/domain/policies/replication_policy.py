from dataclasses import dataclass


@dataclass
class ReplicationDecision:
    replicate_all: bool
    requested_count: int | None


@dataclass(slots=True)
class ReplicationPolicy:
    """Политика определения количества реплик.

    Args:
        default_replicas: Сколько реплик по умолчанию.
        max_replicas: Верхняя граница.
    """

    default_replicas: int = 1
    max_replicas: int = 5

    def resolve_count(
        self, replication_decision: ReplicationDecision, available_nodes: int
    ) -> int:
        """Определяет число реплик по BRS.

        Args:
            replication_decision: Данные о репликациях
            available_nodes: Сколько узлов доступно.

        Returns:
            Число реплик r (>=1).
        """
        if available_nodes <= 0:
            return 0

        if replication_decision.replicate_all:
            return min(available_nodes, self.max_replicas)

        if replication_decision.requested_count is None:
            return min(self.default_replicas, available_nodes)

        return min(
            replication_decision.requested_count, available_nodes, self.max_replicas
        )
