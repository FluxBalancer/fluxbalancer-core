import numpy as np

from modules.decision.domain.normalization import normalize_cost
from modules.types.numpy import Matrix


def test_normalize_cost_range():
    X: Matrix = np.array([
        [10, 100],
        [20, 200],
        [30, 300],
    ], dtype=float)

    Y: Matrix = normalize_cost(X)

    assert Y.max() <= 1
    assert Y.min() >= 0
    assert np.allclose(Y[0], [0, 0])
    assert np.allclose(Y[-1], [1, 1])


def test_normalize_cost_zero_division():
    X: Matrix = np.array([
        [1, 1],
        [1, 1],
        [1, 1],
        [1, 1],
        [1, 1],
    ], dtype=float)

    Y: Matrix = normalize_cost(X)

    assert not np.isnan(Y).any()
    assert all(np.allclose(y, [0, 0]) for y in Y)


def test_normalize_double_cost_zero():
    X: Matrix = np.array([
        [1, 1],
        [2, 2],
        [1, 1],
    ], dtype=float)

    Y: Matrix = normalize_cost(X)

    assert np.allclose(Y[0], [0, 0])
    assert np.allclose(Y[0], Y[-1])
