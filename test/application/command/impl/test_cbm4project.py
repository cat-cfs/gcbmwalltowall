from pathlib import Path
from arrow_space.raster_indexed_dataset import RasterIndexedDataset


def test_properties(cbm4_project, cbm4_config_path):
    assert cbm4_project.config_path == cbm4_config_path
    assert isinstance(cbm4_project.inventory_dataset, RasterIndexedDataset)
    assert isinstance(cbm4_project.disturbance_dataset, RasterIndexedDataset)
    assert cbm4_project.t0_year == 2009
    assert (
        isinstance(cbm4_project.cbm_defaults_path, Path)
        and cbm4_project.cbm_defaults_path.is_file()
    )
    assert cbm4_project.chunk_size == (2500, 2500)
    assert (
        isinstance(cbm4_project.disturbance_order, list)
        and len(cbm4_project.disturbance_order) > 1
    )


def test_get_max_transition_id(cbm4_project):
    assert cbm4_project.get_max_transition_id() == 1


def test_extract_bounding_box(cbm4_project):
    bbox_path = cbm4_project.extract_bounding_box()
    assert Path(bbox_path).is_file()
