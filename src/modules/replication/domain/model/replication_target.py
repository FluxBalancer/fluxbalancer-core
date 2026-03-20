from dataclasses import dataclass


@dataclass(slots=True)
class ReplicationTarget:
    """
    Цель репликации.

    Args:
        node_id: Узел.
        host: Хост.
        port: Порт.
        delay_ms: Задержка запуска.
        max_inflight:
            Максимально допустимое число уже идущих запросов на target.node_id
            в момент старта ЭТОЙ реплики.
            Если текущее число in-flight больше этого лимита, реплика не запускается.
    """

    node_id: str
    host: str
    port: int
    delay_ms: int = 0
    max_inflight: int | None = None
