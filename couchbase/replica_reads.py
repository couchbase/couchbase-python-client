from enum import IntEnum


class ReadPreference(IntEnum):
    NO_PREFERENCE = 0
    SELECTED_SERVER_GROUP = 1
