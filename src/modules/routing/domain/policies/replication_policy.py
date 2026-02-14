class ReplicationPolicy:
    """
    Определяет количество репликаций.
    """

    def __init__(self, default_replicas: int = 1, max_replicas: int = 5):
        self.default = default_replicas
        self.max = max_replicas

    def resolve(
        self,
        available_nodes: int,
        *,
        replicate_all: bool = False,
        replications_count: int | None = None,
    ) -> int:
        if replicate_all:
            return min(available_nodes, self.max)

        if replications_count is None:
            return min(self.default, available_nodes)

        return min(replications_count, available_nodes, self.max)
