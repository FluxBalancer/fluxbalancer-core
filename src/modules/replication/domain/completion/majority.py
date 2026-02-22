from .base import CompletionPolicy, ReplicaReply


class MajorityPolicy(CompletionPolicy):
    """Голосование большинством среди полученных ответов.

    Завершает выполнение при достижении:
        ceil(n / 2) + 1
    """

    def __init__(self) -> None:
        self._received: list[ReplicaReply] = []
        self._counts: dict[str, int] = {}

    def push(self, reply: ReplicaReply) -> None:
        self._received.append(reply)

        if reply.ok:
            self._counts[reply.value] = self._counts.get(reply.value, 0) + 1

    def is_done(self) -> bool:
        n = len(self._received)
        if n == 0:
            return False

        q_min = (n // 2) + 1
        return any(c >= q_min for c in self._counts.values())

    def choose(self) -> ReplicaReply:
        if not self.is_done():
            raise RuntimeError("MajorityPolicy: большинство не достигнуто")

        n = len(self._received)
        q_min = (n // 2) + 1

        winners = {v for v, c in self._counts.items() if c >= q_min}

        candidates = [r for r in self._received if r.ok and r.value in winners]

        return min(candidates, key=lambda r: r.latency_ms)
