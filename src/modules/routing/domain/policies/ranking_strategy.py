from typing import Protocol

from src.modules.types.numpy import Matrix, Vector


class RankingStrategy(Protocol):
    """Стратегия ранжирования альтернатив (узлов)."""

    def score_all(self, scores: Matrix, weights: Vector) -> Vector:
        """Возвращает score для всех альтернатив.

        Args:
            scores: Матрица альтернатив (m × n).
            weights: Вектор весов критериев (n).

        Returns:
            Вектор score длины m.
        """

    def choose(self, scores: Matrix, weights: Vector) -> int:
        """Возвращает индекс лучшей альтернативы."""
