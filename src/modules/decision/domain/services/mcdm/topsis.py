import numpy as np

from src.modules.types.numpy import Matrix, Vector


# Время выполнения: 0.000999450684 секунд
def topsis(x_matrix: Matrix, w: Vector) -> Vector:
    """Выбор лучшей альтернативы методом TOPSIS.

    Все критерии рассматриваются как критерии «издержек»
    (меньшее значение — лучшее)

    Args:
        x_matrix: Матрица решений размера (m, n),
            где m — число альтернатив, n — число критериев
        w: Вектор весов критериев размера (n,)

    Returns:
        Индекс (0-based) наилучшей альтернативы
    """

    # Нормализация матрицы решений
    # Каждый столбец нормируется по евклидовой норме
    norm: Vector = np.linalg.norm(x_matrix, axis=0)
    norm[norm == 0] = 1.0  # защита от деления на 0 для константных критериев

    R: Matrix = x_matrix / norm

    # Взвешивание нормализованной матрицы
    V: Matrix = R * w

    # Определение идеального и анти-идеального решений
    # Для критериев-издержек:
    #   - идеальное решение — минимум по столбцу
    #   - анти-идеальное — максимум по столбцу
    A_pos: Vector = V.min(axis=0)
    A_neg: Vector = V.max(axis=0)

    # Расчёт расстояний до идеального и анти-идеального решений
    D_pos: Vector = np.linalg.norm(V - A_pos, axis=1)
    D_neg: Vector = np.linalg.norm(V - A_neg, axis=1)

    # Вычисление относительной близости к идеальному решению
    # C_i = D_neg / (D_pos + D_neg)
    denominator: Vector = D_pos + D_neg
    C: Vector = np.divide(
        D_neg, denominator, out=np.zeros_like(denominator), where=denominator != 0
    )

    # Выбор альтернативы с максимальной близостью к идеалу
    return C
