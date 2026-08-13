from gcbmwalltowall.runner.cbm4 import load_config, run


def test_load_config(cbm4_config_path):
    json_config = load_config(str(cbm4_config_path))
    assert isinstance(json_config, tuple)


def test_run_libcbm(cbm4_project_copy):
    cbm4_config_path = str(cbm4_project_copy.joinpath("cbm4_config.json"))
    run(cbm4_config_path)
