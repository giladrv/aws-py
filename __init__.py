# Standard
from enum import Enum
# External
from botocore import exceptions

def enval(v: Enum):
    return v.value if isinstance(v, Enum) else v
