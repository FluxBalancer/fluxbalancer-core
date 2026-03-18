from .base import CompletionPolicy, ReplicaReply


class MajorityPolicy(CompletionPolicy):
    """
    Настоящее majority:
    нужно получить большинство от ОБЩЕГО числа запущенных реплик.

    Для n_total:
        quorum = floor(n_total / 2) + 1
    """

    def __init__(self, expected_n: int) -> None:
        if expected_n <= 0:
            raise ValueError("expected_n должно быть > 0")

        self.expected_n = expected_n
        self.required = (expected_n // 2) + 1

        self.replies: list[ReplicaReply] = []
        self._counts: dict[str, int] = {}

    def push(self, reply: ReplicaReply) -> None:
        self.replies.append(reply)

        if reply.ok:
            self._counts[reply.value] = self._counts.get(reply.value, 0) + 1

    def is_done(self) -> bool:
        return any(count >= self.required for count in self._counts.values())

    def choose(self) -> ReplicaReply:
        if not self.is_done():
            raise RuntimeError("MajorityPolicy: большинство не достигнуто")

        winners = {
            value for value, count in self._counts.items() if count >= self.required
        }
        candidates = [
            reply for reply in self.replies if reply.ok and reply.value in winners
        ]

        return min(candidates, key=lambda r: r.latency_ms)
