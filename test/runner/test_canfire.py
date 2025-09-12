from pathlib import Path

default_config_path = Path(__file__).parent.joinpath("resources", "cbm4_config.json")


def test_load_config(): ...
