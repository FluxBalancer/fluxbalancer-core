import numpy as np

from modules.types.numpy import Matrix, Vector


def normalize_cost(x: Matrix) -> Matrix:
    """
    Min-max нормализация для cost-критериев.
    После нормализации:
        0 — лучшая альтернатива
        1 — худшая
    """

    x = np.asarray(x, dtype=float)

    col_min: Vector = x.min(axis=0)
    col_max: Vector = x.max(axis=0)

    denominator: Vector = np.where(col_max == col_min, 1.0, col_max - col_min)

    return (x - col_min) / denominator
