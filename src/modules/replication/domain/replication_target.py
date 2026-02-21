from dataclasses import dataclass


@dataclass(slots=True)
class ReplicationTarget:
    """Цель репликации.

    Args:
        node_id: Узел.
        host: Хост.
        port: Порт.
        delay_ms: Задержка запуска (hedged/speculative). Для fixed = 0.
    """

    node_id: str
    host: str
    port: int
    delay_ms: int = 0
