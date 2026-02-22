from .base import CompletionPolicy, ReplicaReply


class KOutOfNPolicy(CompletionPolicy):
    """Схема k-out-of-n.

    Завершает выполнение после получения k валидных ответов.

    Args:
        k: Минимальное число валидных ответов.
    """

    def __init__(self, k: int) -> None:
        if k <= 0:
            raise ValueError("k должно быть > 0")
        self.k = k
        self._valid: list[ReplicaReply] = []

    def push(self, reply: ReplicaReply) -> None:
        if reply.ok:
            self._valid.append(reply)

    def is_done(self) -> bool:
        return len(self._valid) >= self.k

    def choose(self) -> ReplicaReply:
        if not self.is_done():
            raise RuntimeError("KOutOfNPolicy: недостаточно валидных ответов")
        return min(self._valid, key=lambda r: r.latency_ms)
