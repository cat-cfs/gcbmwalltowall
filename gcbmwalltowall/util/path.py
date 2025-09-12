import os
import pathlib


def Path(*p) -> pathlib.Path:
    return pathlib.Path(pathlib.PureWindowsPath(*p).as_posix())


def relpath(path, start):
    return pathlib.Path(os.path.relpath(path, start)).as_posix()
