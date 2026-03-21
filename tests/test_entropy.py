import numpy as np

from modules.decision.domain.services.entropy import entropy_weights
from modules.types.numpy import Vector


def test_entropy_weights_basic():
    X = np.array([
        [1, 2],
        [2, 3],
        [3, 4]
    ], dtype=float)

    w: Vector = entropy_weights(X)

    assert len(w) == 2
    assert w.sum() == 1.0
    assert all(w > 0)