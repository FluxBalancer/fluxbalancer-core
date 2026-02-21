import numpy as np

from src.modules.types.numpy import Matrix, Vector, BoolVector, IntVector


# Время выполнения: 0.006000 секунд
def airm(
    x_matrix: Matrix,
    w: Vector,
    *,
    n_iter: int = 1000,
    alpha_scale: float = 5,
    benefit_mask=None,
    random_state: int | None = None,
) -> Vector:
    """Выбор лучшей альтернативы методом AIRM (Aggregated Indices Randomization Method).

    Метод основан на многократной рандомизации весов критериев
    (распределение Дирихле) и подсчёте частоты доминирования
    альтернатив по агрегированному индексу.

    Args:
        x_matrix: Матрица решений размера (m, n),
            где m — число альтернатив, n — число критериев.
        w: Номинальный вектор весов критериев размера (n,).
            Все веса должны быть неотрицательными.
        n_iter: Число итераций Монте-Карло.
            Чем больше значение, тем стабильнее результат.
        alpha_scale: Масштаб параметров распределения Дирихле.
            Большие значения фиксируют веса ближе к номинальным,
            малые — увеличивают их вариативность.
        benefit_mask: Логический вектор размера (n,),
            где True обозначает критерий «выгоды» (больше — лучше),
            а False — критерий «издержек» (меньше — лучше).
            Если не задан, все критерии считаются издержками.
        random_state: Зерно генератора случайных чисел
            для воспроизводимости эксперимента.

    Returns:
        Индекс (0-based) альтернативы с наибольшей
        вероятностью доминирования.
    """
    if (w < 0).any():
        raise ValueError("все веса должны быть > 0")

    if benefit_mask is None:
        benefit_mask = np.zeros(len(w), dtype=bool)

    rng = np.random.default_rng(random_state)
    x: Matrix = np.asarray(x_matrix, dtype=float)
    m, n = x.shape

    benefit_mask: BoolVector = np.asarray(benefit_mask, dtype=bool)
    if benefit_mask.shape != (n,):
        raise ValueError("benefit_mask должен иметь длину n")

    # Приведение всех критериев к типу «выгода»
    # Для критериев-издержек выполняется инверсия
    x_adj: Matrix = x.copy()
    if (~benefit_mask).any():
        x_adj[:, ~benefit_mask] = (
            x_adj[:, ~benefit_mask].max(axis=0) - x_adj[:, ~benefit_mask]
        )

    # Нормализация критериев в диапазон [0, 1]
    col_min: Vector = x_adj.min(axis=0)
    col_max: Vector = x_adj.max(axis=0)
    denominator: Vector = np.where(col_max == col_min, 1.0, col_max - col_min)
    x_norm: Matrix = (x_adj - col_min) / denominator

    # Рандомизация весов и подсчёт числа побед каждой альтернативы
    counts: IntVector = np.zeros(m, dtype=int)
    alpha: Vector = w * alpha_scale

    for _ in range(n_iter):
        w_rand: Vector = rng.dirichlet(alpha)
        scores: Vector = x_norm @ w_rand  # агрегированный индекс
        counts[scores.argmax()] += 1  # победитель этой итерации

    return counts.astype(float) / float(n_iter)
