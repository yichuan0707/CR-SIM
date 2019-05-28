from enum import Enum


class UnitState(Enum):
    Normal = 0
    Crashed = 1
    Corrupted = 2
    LatentError = 3
