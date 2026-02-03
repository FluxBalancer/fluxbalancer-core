from starlette.datastructures import Headers
from starlette.requests import Request

from src.modules.routing.application.dto.brs import BRSRequest


class BRSParser:
    """Парсер Balancer and Replications Protocol (BRS).

    Отвечает за извлечение и валидацию BRS-заголовков из HTTP-запроса.
    Реализует строгий контракт: при отсутствии обязательных заголовков
    или некорректных значениях выбрасывает исключение.

    Поддерживаемые заголовки:
      - X-Service (str, обязателен)
      - X-Replications-Count (int | "true", опционален)
      - X-Replications-All (bool, опционален)
      - X-Replications-Strategy (RankingStrategy, опционален)
      - X-Balancer-Deadline (int, миллисекунды, обязателен)
      - X-Balancer-Strategy (str, опционален)
      X-Weights-Strategy: (str, опционален)
    """

    DEFAULT_REPLICATIONS = 3

    @classmethod
    def parse(cls, request: Request) -> BRSRequest:
        """Парсит BRS-заголовки из HTTP-запроса.

        Args:
            request: Входящий HTTP-запрос Starlette/FastAPI.

        Returns:
            Валидированный объект BRSRequest.

        Raises:
            ValueError: Если отсутствует обязательный заголовок
                или значение заголовка некорректно.
        """
        headers: Headers = request.headers

        service = cls._parse_service(headers)
        deadline_ms = cls._parse_deadline(headers)
        replicate_all = cls._parse_replicate_all(headers)
        replications_count = cls._parse_replications_count(headers)
        balancer_strategy = cls._parse_strategy(headers)
        weights_strategy = cls._parse_weights_strategy(headers)

        return BRSRequest(
            service=service,
            replications_count=replications_count,
            replicate_all=replicate_all,
            deadline_ms=deadline_ms,
            balancer_strategy_name=balancer_strategy,
            weights_strategy_name=weights_strategy,
            replication_strategy_name=None,
        )

    @staticmethod
    def _parse_weights_strategy(headers: Headers) -> str | None:
        value: str | None = headers.get("X-Weights-Strategy")
        if value is None:
            return None

        value = value.lower().strip()
        if not value:
            raise ValueError("BRS: X-Weights-Strategy не должен быть пустым")
        return value

    @staticmethod
    def _parse_strategy(headers: Headers) -> str | None:
        strategy: str | None = headers.get("X-Balancer-Strategy")
        if strategy is None:
            return None

        strategy = strategy.lower().strip()
        if not strategy:
            raise ValueError("BRS: X-Balancer-Strategy не должен быть пустым")

        return strategy

    @staticmethod
    def _parse_service(headers: Headers) -> str:
        service: str | None = headers.get("X-Service")
        if not service:
            raise ValueError("BRS: отсутствует обязательный заголовок X-Service")
        return service

    @staticmethod
    def _parse_deadline(headers: Headers) -> int:
        deadline_raw: str | None = headers.get("X-Balancer-Deadline")
        if deadline_raw is None:
            raise ValueError(
                "BRS: отсутствует обязательный заголовок X-Balancer-Deadline"
            )

        try:
            deadline_ms = int(deadline_raw)
            if deadline_ms <= 0:
                raise ValueError
        except ValueError:
            raise ValueError(
                "BRS: X-Balancer-Deadline должен быть положительным целым числом (мс)"
            )

        return deadline_ms

    @staticmethod
    def _parse_replicate_all(headers: Headers) -> bool:
        value: str = headers.get("X-Replications-All", "false").lower()
        if value not in {"true", "false"}:
            raise ValueError("BRS: X-Replications-All должен быть 'true' или 'false'")
        return value == "true"

    @staticmethod
    def _parse_replications_count(headers: Headers) -> int | None:
        value: str | None = headers.get("X-Replications-Count")
        if value is None:
            return None

        if value.lower() == "true":
            return BRSParser.DEFAULT_REPLICATIONS

        try:
            count = int(value)
            if count <= 0:
                raise ValueError
        except ValueError:
            raise ValueError(
                "BRS: X-Replications-Count должен быть положительным целым числом или 'true'"
            )

        return count
