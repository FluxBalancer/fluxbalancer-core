from typing import Protocol

import numpy as np

from src.modules.types.numpy import Matrix, Vector


class WeightsStrategy(Protocol):
    def compute(self, matrix: Matrix) -> Vector: ...
