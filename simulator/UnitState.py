from enum import Enum


class UnitState(Enum):
    Normal = 1
    Crashed = 0
    Corrupted = -1
    LatentError = -2
