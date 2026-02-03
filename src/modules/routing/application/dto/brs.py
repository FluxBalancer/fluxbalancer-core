from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BRSRequest:
    """DTO запроса Balancer and Replications Protocol (BRS).

    Содержит данные, извлечённые из BRS-заголовков HTTP-запроса.
    Объект является неизменяемым (immutable) и не содержит бизнес-логики,
    за исключением явного разрешения стратегии балансировки через реестр.

    Attributes:
        service: Название сервиса, к которому относится запрос.
        replications_count: Количество репликаций запроса.
            Если None — количество определяется балансировщиком.
        replicate_all: Признак репликации запроса на все доступные узлы сервиса.
        deadline_ms: Дедлайн выполнения запроса в миллисекундах.
        balancer_strategy_name: Имя стратегии балансировки (идентификатор),
            используемое для разрешения стратегии через реестр.
        replication_strategy_name: Имя стратегии репликации
        weights_strategy_name: Имя стратегии расчёта весов
    """

    service: str | None
    replications_count: int | None
    replicate_all: bool
    deadline_ms: int | None

    balancer_strategy_name: str | None
    weights_strategy_name: str | None
    replication_strategy_name: str | None
