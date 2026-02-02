import numpy as np

from src.modules.mcdm.algorithms.topsis import topsis


def test_topsis_simple():
    X = np.array([[1, 2], [2, 1], [1.5, 1.5]])
    w = np.array([0.3, 0.7])
    best = topsis(X, w)
    assert best == 1
