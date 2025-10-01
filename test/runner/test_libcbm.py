import shutil
from pathlib import Path

from gcbmwalltowall.runner.cbm4 import load_config, run  # type: ignore

standalone_project_path = Path(__file__).parent.joinpath("resources", "standalone")
results_dir = Path(__file__).parent.joinpath("run_results")


def test_load_config():
    json_config = load_config(
        str(standalone_project_path.joinpath("cbm4_config.json")),
    )

    assert isinstance(json_config, tuple)


def test_run_libcbm():
    dst = results_dir.joinpath("test_run_libcbm")
    if dst.exists():
        shutil.rmtree(dst)

    project = Path(shutil.copytree(standalone_project_path, dst))
    cbm4_config_path = str(project.joinpath("cbm4_config.json"))

    run(cbm4_config_path)
