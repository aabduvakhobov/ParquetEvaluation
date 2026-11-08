import os
import subprocess
from enum import IntEnum
import sqlite3


class ZstdLevel(IntEnum):
    LEVEL_1  = 1
    LEVEL_2  = 2
    LEVEL_3  = 3
    LEVEL_4  = 4
    LEVEL_5  = 5
    LEVEL_6  = 6
    LEVEL_7  = 7
    LEVEL_8  = 8
    LEVEL_9  = 9
    LEVEL_10 = 10
    LEVEL_11 = 11
    LEVEL_12 = 12
    LEVEL_13 = 13
    LEVEL_14 = 14
    LEVEL_15 = 15
    LEVEL_16 = 16
    LEVEL_17 = 17
    LEVEL_18 = 18
    LEVEL_19 = 19
    LEVEL_20 = 20
    LEVEL_21 = 21
    LEVEL_22 = 22


class BrotliLevel(IntEnum):
    ZERO = 0
    ONE = 1
    TWO = 2
    THREE = 3
    FOUR = 4
    FIVE = 5
    SIX = 6
    SEVEN = 7
    EIGHT = 8
    NINE = 9
    TEN = 10
    ELEVEN = 11


def get_git_commit_hash():
    """Commit of this repository (not of the working directory), or 'unknown' outside a git checkout."""
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            stderr=subprocess.DEVNULL
        ).decode("utf-8").strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"
