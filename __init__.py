# Standard
from enum import Enum

def enval(v: Enum):
    return v.value if isinstance(v, Enum) else v
