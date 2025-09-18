import os
import pathlib

def Path(*p):
    return pathlib.Path(pathlib.PureWindowsPath(*p).as_posix())

def relpath(path, start):
    return Path(os.path.relpath(path, start)).as_posix()
