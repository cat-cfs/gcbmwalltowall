import os
from pathlib import Path, PureWindowsPath


def Path(*p) -> Path:
    return Path(PureWindowsPath(*p).as_posix())

<<<<<<< HEAD

def Path(*p) -> pathlib.Path:
    return pathlib.Path(pathlib.PureWindowsPath(*p).as_posix())
=======
>>>>>>> ed40223 (canfire runner)


def relpath(path, start):
    return pathlib.Path(os.path.relpath(path, start)).as_posix()
