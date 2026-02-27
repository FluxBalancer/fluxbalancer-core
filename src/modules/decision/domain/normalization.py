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
    x = np.where(np.isfinite(x), x, np.nan)

    col_min: Vector = x.min(axis=0)
    col_max: Vector = x.max(axis=0)

    denominator: Vector = col_max - col_min
    # где denominator невалиден/0 -> ставим 1, чтобы избежать nan/inf
    denominator: Vector = np.where(
        ~np.isfinite(denominator) | (denominator == 0),
        1.0,
        denominator
    )

    y = (x - col_min) / denominator
    # оставшиеся nan -> 1 (как “худшее” для cost), либо 0 если хотите “нейтраль”
    y = np.where(np.isfinite(y), y, 1.0)

    return y
