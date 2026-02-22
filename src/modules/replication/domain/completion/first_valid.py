from .base import CompletionPolicy, ReplicaReply


class FirstValidPolicy(CompletionPolicy):
    """Завершение по первому валидному ответу (k=1).

    Используется для подавления хвостовых задержек
    (hedged / speculative execution).
    """

    def __init__(self) -> None:
        self._winner: ReplicaReply | None = None

    def push(self, reply: ReplicaReply) -> None:
        if self._winner is None and reply.ok:
            self._winner = reply

    def is_done(self) -> bool:
        return self._winner is not None

    def choose(self) -> ReplicaReply:
        if self._winner is None:
            raise RuntimeError("FirstValidPolicy: нет валидного ответа")
        return self._winner
