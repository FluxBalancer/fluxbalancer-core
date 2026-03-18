from .base import CompletionPolicy, ReplicaReply


class QuorumPolicy(CompletionPolicy):
    """
    Кворум как "нужно q одинаковых валидных ответов".

    Это более нормальная семантика для реплицированного прокси,
    чем старая версия "сначала собрать k ответов, потом считать majority".
    """

    def __init__(self, quorum_size: int) -> None:
        if quorum_size <= 0:
            raise ValueError("quorum_size должно быть > 0")

        self.quorum_size = quorum_size
        self.replies: list[ReplicaReply] = []
        self._counts: dict[str, int] = {}

    def push(self, reply: ReplicaReply) -> None:
        self.replies.append(reply)

        if reply.ok:
            self._counts[reply.value] = self._counts.get(reply.value, 0) + 1

    def is_done(self) -> bool:
        return any(count >= self.quorum_size for count in self._counts.values())

    def choose(self) -> ReplicaReply:
        if not self.is_done():
            raise RuntimeError("QuorumPolicy: кворум не достигнут")

        winners = {
            value for value, count in self._counts.items() if count >= self.quorum_size
        }
        candidates = [
            reply for reply in self.replies if reply.ok and reply.value in winners
        ]

        return min(candidates, key=lambda r: r.latency_ms)
