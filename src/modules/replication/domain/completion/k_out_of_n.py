from .base import CompletionPolicy, ReplicaReply


class KOutOfNPolicy(CompletionPolicy):
    """Завершает выполнение после получения k валидных ответов."""

    def __init__(self, k: int) -> None:
        if k <= 0:
            raise ValueError("k должно быть > 0")
        self.k = k
        self.replies: list[ReplicaReply] = []

    def push(self, reply: ReplicaReply) -> None:
        self.replies.append(reply)

    def is_done(self) -> bool:
        valid_count = sum(1 for reply in self.replies if reply.ok)
        return valid_count >= self.k

    def choose(self) -> ReplicaReply:
        valid = [reply for reply in self.replies if reply.ok]
        if len(valid) < self.k:
            raise RuntimeError("KOutOfNPolicy: недостаточно валидных ответов")

        return min(valid, key=lambda r: r.latency_ms)
