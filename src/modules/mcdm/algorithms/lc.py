import numpy as np

from src.modules.types.numpy import Matrix, Vector


# Время выполнения: 0.006000 секунд
def lc(
    x_matrix: Matrix,
    w: Vector,
) -> Vector:
    """Выбор лучшей альтернативы методом взвешенной суммы (Linear Scalarization).

    Метод основан на линейной агрегации значений критериев
    с использованием заданного вектора весов.

    Args:
        x_matrix: Матрица решений размера (m, n),
            где m — число альтернатив, n — число критериев.
        w: Нормированный вектор весов критериев размера (n,).
            Все веса должны быть неотрицательными.

    Returns:
        Индекс (0-based) альтернативы с наибольшим
        значением агрегированного показателя.
    """
    if (w < 0).any():
        raise ValueError("все веса должны быть > 0")

    # Линейная агрегация критериев
    # scores_i = sum_j (x_ij * w_j)
    scores: Vector = x_matrix @ w

    return scores
