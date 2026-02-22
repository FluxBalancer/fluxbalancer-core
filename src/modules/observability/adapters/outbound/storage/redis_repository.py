import json

import numpy as np
from redis.asyncio.client import Redis

from src.modules.observability.application.ports.metrics_repository import (
    MetricsRepository,
)
from src.modules.observability.domain.node_metrics import NodeMetrics


class RedisMetricsRepository(MetricsRepository):
    """
    Репозиторий метрик узлов на базе Redis.

    Реализация порта MetricsRepository, использующая Redis
    в качестве внешнего хранилища метрик с поддержкой:
      - последних значений метрик по узлам,
      - ограниченной истории метрик,
      - скользящего окна latency-событий с расчётом p95.

    Структура данных в Redis (по каждому node_id):

        metrics:{node_id}:latest
            HASH — последние метрики узла.

        metrics:{node_id}:history
            LIST — история метрик (JSON),
            ограниченная параметром history_limit.

        metrics:{node_id}:latency
            LIST — latency-события (float, ms),
            ограниченная параметром latency_window.

    Репозиторий:
      - потокобезопасен на уровне Redis,
      - подходит для многопроцессного и распределённого запуска,
      - может переживать рестарты приложения.

    Args:
        redis_client: Клиент Redis (redis.Redis).
        history_limit: Максимальное число записей истории
            метрик для каждого узла.
        latency_window: Размер скользящего окна latency
            (используется для расчёта p95).
        prefix: Префикс ключей Redis.

    Notes:
        - Latency хранится отдельно от основных метрик,
          агрегируется при чтении.
        - Для вычисления latency используется 95-й перцентиль.
        - Репозиторий не управляет TTL ключей (можно добавить при необходимости).
    """

    def __init__(
        self,
        redis_client: Redis,
        history_limit: int = 32,
        latency_window: int = 100,
        prefix: str = "metrics",
    ):
        self.redis = redis_client
        self.history_limit = history_limit
        self.latency_window = latency_window
        self.prefix = prefix

    def _k_latest(self, node_id: str) -> str:
        """
        Формирует ключ Redis для хранения
        последних метрик узла.

        Args:
            node_id: Идентификатор узла.

        Returns:
            Ключ Redis для HASH с последними метриками.
        """
        return f"{self.prefix}:{node_id}:latest"

    def _k_history(self, node_id: str) -> str:
        """
        Формирует ключ Redis для хранения
        истории метрик узла.

        Args:
            node_id: Идентификатор узла.

        Returns:
            Ключ Redis для LIST с историей метрик.
        """
        return f"{self.prefix}:{node_id}:history"

    def _k_latency(self, node_id: str) -> str:
        """
        Формирует ключ Redis для хранения
        latency-событий узла.

        Args:
            node_id: Идентификатор узла.

        Returns:
            Ключ Redis для LIST с latency-событиями.
        """
        return f"{self.prefix}:{node_id}:latency"

    def _serialize(self, m: NodeMetrics) -> str:
        """
        Сериализует объект NodeMetrics в JSON-строку.

        Args:
            m: Объект метрик узла.

        Returns:
            JSON-представление метрик.
        """
        return json.dumps(
            {
                "timestamp": m.timestamp,
                "node_id": m.node_id,
                "cpu_util": m.cpu_util,
                "mem_util": m.mem_util,
                "net_in_bytes": m.net_in_bytes,
                "net_out_bytes": m.net_out_bytes,
                "latency_ms": m.latency_ms,
            }
        )

    def _deserialize(self, raw: str) -> NodeMetrics:
        """
        Десериализует JSON-строку в объект NodeMetrics.

        Args:
            raw: JSON-строка с метриками.

        Returns:
            Восстановленный объект NodeMetrics.
        """
        data = json.loads(raw)
        return NodeMetrics(**data)

    async def upsert(self, metrics: NodeMetrics) -> None:
        """
        Сохраняет метрики узла в Redis.

        Операция атомарна (транзакция):
          - обновляет последние метрики узла
          - добавляет запись в историю
          - обрезает историю до заданного лимита

        Args:
            metrics: Метрики узла для сохранения.
        """
        async with self.redis.pipeline(transaction=True) as pipe:
            await (
                pipe.hset(
                    self._k_latest(metrics.node_id),
                    mapping={
                        "timestamp": metrics.timestamp,
                        "node_id": metrics.node_id,
                        "cpu_util": metrics.cpu_util,
                        "mem_util": metrics.mem_util,
                        "net_in_bytes": metrics.net_in_bytes,
                        "net_out_bytes": metrics.net_out_bytes,
                        "latency_ms": metrics.latency_ms or 0.0,
                    },
                )
                .lpush(self._k_history(metrics.node_id), self._serialize(metrics))
                .ltrim(self._k_history(metrics.node_id), 0, self.history_limit - 1)
                .execute()
            )

    async def add_latency(self, node_id: str, latency_ms: float) -> None:
        """
        Добавляет событие latency для узла.

        Значение сохраняется в скользящем окне,
        используемом для расчёта p95.

        Args:
            node_id: Идентификатор узла.
            latency_ms: Задержка в миллисекундах.
        """
        async with self.redis.pipeline(transaction=True) as pipe:
            await pipe.lpush(self._k_latency(node_id), latency_ms).ltrim(
                self._k_latency(node_id), 0, self.latency_window - 1
            ).execute()

    async def _latency_p95(self, node_id: str) -> float | None:
        """
        Вычисляет 95-й перцентиль latency узла.

        Args:
            node_id: Идентификатор узла.

        Returns:
            Значение p95 latency в миллисекундах
            или None, если данных нет.
        """
        values = await self.redis.lrange(self._k_latency(node_id), 0, -1)
        if not values:
            return None

        arr = np.asarray([float(v) for v in values])
        return float(np.percentile(arr, 95))

    async def _with_latency(self, m: NodeMetrics) -> NodeMetrics:
        """
        Обогащает метрики узла агрегированной latency.

        Args:
            m: Метрики узла без агрегированной latency.

        Returns:
            Новый объект NodeMetrics с p95 latency.
        """
        return NodeMetrics(
            timestamp=m.timestamp,
            node_id=m.node_id,
            cpu_util=m.cpu_util,
            mem_util=m.mem_util,
            net_in_bytes=m.net_in_bytes,
            net_out_bytes=m.net_out_bytes,
            latency_ms=await self._latency_p95(m.node_id),
        )

    async def get_latest(self, node_id: str) -> NodeMetrics | None:
        """
        Возвращает последние метрики узла.

        Args:
            node_id: Идентификатор узла.

        Returns:
            Последние метрики узла или None,
            если данных нет.
        """
        data = await self.redis.hgetall(self._k_latest(node_id))
        if not data:
            return None

        metrics = NodeMetrics(
            timestamp=data["timestamp"],
            node_id=data["node_id"],
            cpu_util=float(data["cpu_util"]),
            mem_util=float(data["mem_util"]),
            net_in_bytes=int(data["net_in_bytes"]),
            net_out_bytes=int(data["net_out_bytes"]),
            latency_ms=float(data.get("latency_ms", 0.0)),
        )

        return await self._with_latency(metrics)

    async def get_prev(self, node_id: str) -> NodeMetrics | None:
        """
        Возвращает предыдущий снимок метрик узла.

        Используется для расчёта дельт
        (например, сетевой активности).

        Args:
            node_id: Идентификатор узла.

        Returns:
            Предыдущие метрики узла или None,
            если истории недостаточно.
        """
        items = await self.redis.lrange(self._k_history(node_id), 1, 1)
        if not items:
            return None

        return self._deserialize(items[0])

    async def list_latest(self) -> list[NodeMetrics]:
        """
        Возвращает последние метрики всех узлов.

        Returns:
            Список последних метрик по всем узлам.
        """
        result: list[NodeMetrics] = []

        async for key in self.redis.scan_iter(f"{self.prefix}:*:latest"):
            node_id = key.split(":")[1]
            metrics = await self.get_latest(node_id)
            if metrics:
                result.append(metrics)

        return result
