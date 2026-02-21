import numpy as np

from src.modules.types.numpy import Matrix, Vector, BoolVector, IntVector

_CONC_THRESHOLD: float = 0.6  # порог согласия
_DIS_THRESHOLD: float = 0.4  # порог несогласия


# Время выполнения: 0.001065731049 секунд
def electre(x_matrix: Matrix, w: Vector) -> Vector:
    """
    Индекс лучшего варианта по ELECTRE III (упрощённо)
    """
    m, n = x_matrix.shape
    concordance: Matrix = np.zeros((m, m))
    discordance: Matrix = np.zeros((m, m))

    for i in range(m):
        for j in range(m):
            if i == j:
                continue
            # согласие: сумма весов, где i не хуже j
            mask: BoolVector = x_matrix[i] <= x_matrix[j]  # cost-критерий
            concordance[i, j] = w[mask].sum()

            # несогласие: макс относительного проигрыша
            diff: Vector = (x_matrix[i] - x_matrix[j]) / (
                    x_matrix.max(axis=0) - x_matrix.min(axis=0) + 1e-12
            )
            discordance[i, j] = diff.max()

    outrank: BoolVector = (concordance >= _CONC_THRESHOLD) & (
            discordance <= _DIS_THRESHOLD
    )

    # считаем число «побеждённых» каждой альтернативой
    scores: IntVector = outrank.sum(axis=1)
    return scores.astype(float)
