from typing import List

def schedule(attempt: int, base: int = 5, mult: int = 3, cap: int = 3600) -> int:
    """
    Exponential backoff in seconds: base * mult^(attempt-1), capped.
    attempt >= 1
    """
    delay = base * (mult ** max(0, attempt - 1))
    return min(delay, cap)

def plan(max_retries: int, base: int = 5, mult: int = 3, cap: int = 3600) -> List[int]:
    return [schedule(i, base, mult, cap) for i in range(1, max_retries + 1)]
