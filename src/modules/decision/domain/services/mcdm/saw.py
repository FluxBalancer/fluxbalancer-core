from src.modules.types.numpy import Matrix, Vector


# Время выполнения: 0.000000000000 секунд (очень быстро!)
def saw(x_matrix: Matrix, w: Vector) -> Vector:
    """Возвращает индекс лучшего варианта.

    Args:
        x_matrix: Матрица решений.
        w: Вектор весов.

    Returns:
        Индекс строки-победителя.
    """
    scores: Vector = (x_matrix * w).sum(axis=1)
    return scores
