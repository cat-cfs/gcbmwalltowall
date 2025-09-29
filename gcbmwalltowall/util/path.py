import os
import pathlib
from typing import Any


def Path(*p: Any) -> pathlib.Path:
    return pathlib.Path(pathlib.PureWindowsPath(*p).as_posix())


def relpath(path, start) -> str:
    return Path(os.path.relpath(path, start)).as_posix()
