from typing import Protocol, Any


class MetricExtractorPolicy(Protocol):
    """
    Экстрактор извлекает кусок метрик из raw docker stats.
    Добавление новой метрики = новый класс-экстрактор.
    """

    def extract(self, raw_stats: dict[str, Any]) -> dict[str, float]:
        """
        Возвращает часть метрик.
        Например: {"cpu_util": 0.42}
        """
        ...
