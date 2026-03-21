import numpy as np

from modules.decision.domain.services.entropy import entropy_weights
from modules.types.numpy import Vector, Matrix


def test_entropy_weights_basic():
    X: Matrix = np.array([
        [1, 2],
        [2, 3],
        [3, 4]
    ], dtype=float)

    w: Vector = entropy_weights(X)

    assert len(w) == 2
    assert np.isclose(w.sum(), 1.0)
    assert all(w > 0)
