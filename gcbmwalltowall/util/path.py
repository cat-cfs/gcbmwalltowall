import os
from pathlib import Path, PureWindowsPath


def Path(*p) -> Path:
    return Path(PureWindowsPath(*p).as_posix())


def relpath(path, start):
    return Path(os.path.relpath(path, start)).as_posix()
