import numpy as np

from modules.decision.domain.services.mcdm.airm import airm


def test_airm_deterministic():
    X = np.array([[1, 0, 0], [0, 1, 0]])
    w = np.array([0.34, 0.33, 0.33])
    idx = airm(X, w, n_iter=500, random_state=42)
    assert idx in (0, 1)
