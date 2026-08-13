import shutil
import gcbmwalltowall
from typing import Iterator
from tempfile import TemporaryDirectory
from pathlib import Path
from pytest import fixture
from gcbmwalltowall.application.command.impl.cbm4project import CBM4Project


@fixture(scope="module")
def cbm4_input_path() -> Path:
    return Path(gcbmwalltowall.__file__).parent.parent.joinpath(
        "test", "resources", "standalone"
    )


@fixture(scope="module")
def cbm4_config_path(cbm4_input_path: Path) -> Path:
    return cbm4_input_path.joinpath("cbm4_config.json")


@fixture(scope="module")
def cbm4_project(cbm4_config_path: Path) -> CBM4Project:
    return CBM4Project(cbm4_config_path)


@fixture
def cbm4_project_copy(cbm4_input_path: Path) -> Iterator[Path]:
    with TemporaryDirectory() as tmp:
        yield Path(shutil.copytree(cbm4_input_path, tmp, dirs_exist_ok=True))
