# Copyright (c) 2025 iiPython

# Modules
from enum import Enum

# Handle client-server shared typing
class Response(Enum):
    HELO = 0
    OPOK = 1
    FAIL = 2

class Transaction(Enum):
    PING = 0
    WRIT = 1
    READ = 2
    WIPE = 3
    AUTH = 4
