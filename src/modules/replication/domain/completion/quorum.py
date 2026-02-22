from .base import CompletionPolicy, ReplicaReply


class QuorumPolicy(CompletionPolicy):
    """Кворумная схема завершения.

    Кворум определяется как:
        floor(k / 2) + 1

    Args:
        k: Максимальное число учитываемых ответов.
    """

    def __init__(self, k: int) -> None:
        if k <= 0:
            raise ValueError("k должно быть > 0")
        self.k = k
        self._received: list[ReplicaReply] = []
        self._counts: dict[str, int] = {}

    def push(self, reply: ReplicaReply) -> None:
        if len(self._received) >= self.k:
            return

        self._received.append(reply)

        if reply.ok:
            self._counts[reply.value] = self._counts.get(reply.value, 0) + 1

    def is_done(self) -> bool:
        if len(self._received) < self.k:
            return False

        q_min = (self.k // 2) + 1
        return any(c >= q_min for c in self._counts.values())

    def choose(self) -> ReplicaReply:
        if not self.is_done():
            raise RuntimeError("QuorumPolicy: кворум не достигнут")

        q_min = (self.k // 2) + 1
        winners = {v for v, c in self._counts.items() if c >= q_min}

        candidates = [r for r in self._received if r.ok and r.value in winners]

        return min(candidates, key=lambda r: r.latency_ms)
