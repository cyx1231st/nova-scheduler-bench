from enum import Enum


class Release(Enum):
    # default
    LATEST = 13.5
    MITAKA = 13
    KILO = 11
    PROTOTYPE = "prototype"


DEFAULT_RELEASE = Release.LATEST
