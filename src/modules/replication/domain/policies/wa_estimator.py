from typing import Protocol


class WAEstimator(Protocol):
    """Оценщик прироста work amplification для добавления r-й реплики."""

    def delta_wa(
        self,
        *,
        delay_ms: float,
        prev_finish_hat_ms: float,
        active_prefix: int,
        delays_ms: list[int],
    ) -> float: ...
