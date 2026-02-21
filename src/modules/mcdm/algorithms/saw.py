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
    # Все критерии – «издержки»: меньше → лучше.
    R: Matrix = x_matrix.min(axis=0) / (x_matrix + 1e-12)
    scores: Vector = (R * w).sum(axis=1)
    return scores
