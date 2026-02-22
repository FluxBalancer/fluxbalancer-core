def ema(prev: float, cur: float, alpha: float) -> float:
    return cur if prev == 0 else alpha * cur + (1 - alpha) * prev
