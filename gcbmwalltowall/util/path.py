import pathlib

def Path(*p):
    return pathlib.Path(pathlib.PureWindowsPath(*p).as_posix())
