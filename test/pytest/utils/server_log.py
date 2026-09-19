"""The storage server's log file, which every start appends to."""
import os
from pathlib import Path

PATH = Path(__file__).resolve().parents[3] / "helios_data" / "logs" / "helios.log"


def size():
    """Bytes the log holds, zero when no start has written one yet."""
    return os.path.getsize(PATH) if os.path.exists(PATH) else 0


def read_since(offset):
    """The text appended past `offset`, so a caller reads its own start only."""
    if not os.path.exists(PATH):
        return ""
    with open(PATH, "rb") as log:
        log.seek(offset)
        return log.read().decode("utf-8", errors="replace")
