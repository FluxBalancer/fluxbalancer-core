from dataclasses import dataclass
from typing import Protocol


@dataclass(slots=True)
class ReplicaReply:
    """Результат выполнения одной реплики запроса.

    Attributes:
        node_id: Идентификатор узла.
        ok: Признак валидности ответа.
        value: Нормализованное значение ответа (для сравнения).
        raw_body: Сырые байты ответа.
        status: HTTP-статус.
        latency_ms: Задержка выполнения в миллисекундах.
    """

    node_id: str
    ok: bool
    value: str
    raw_body: bytes
    status: int
    latency_ms: float


class CompletionPolicy(Protocol):
    """Интерфейс политики завершения реплицированного запроса."""

    def push(self, reply: ReplicaReply) -> None:
        """Добавляет очередной ответ реплики."""
        ...

    def is_done(self) -> bool:
        """Возвращает True, если можно завершать выполнение."""
        ...

    def choose(self) -> ReplicaReply:
        """Возвращает выбранный итоговый ответ."""
        ...
