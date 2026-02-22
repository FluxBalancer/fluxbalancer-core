import numpy as np
from numpy.typing import NDArray

from src.modules.types.numpy import Matrix, Vector


def entropy_weights(x_matrix: Matrix) -> Vector:
    """
    Если столбец-критерий мало изменяется от варианта к варианту, он не несёт новой информации: энтропия почти максимальна, полезность мала.

    Чем больше «разброс» значений, тем ниже энтропия и выше информационная полезность.
    """
    m, n = x_matrix.shape
    if m == 1:
        return np.full(n, 1.0 / n)

    x_matrix: Matrix = x_matrix + 1e-12
    p: Vector = x_matrix / x_matrix.sum(axis=0, keepdims=True)
    k: float = 1.0 / np.log(m)

    # Формула Шеннона
    e: float = (-k) * (p * np.log(p)).sum(axis=0)

    d: Vector = np.clip(1.0 - e, 0.0, None)
    return d / d.sum()
