from typing import Protocol


class WAEstimator(Protocol):
    """Оценщик прироста work amplification для добавления r-й реплики."""

    def delta_wa(self, r: int) -> float:
        """Возвращает ожидаемый прирост WA при добавлении r-й реплики."""
        ...
