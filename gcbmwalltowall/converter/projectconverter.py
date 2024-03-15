import shutil
import pandas as pd
from tempfile import TemporaryDirectory
from contextlib import contextmanager
from sqlalchemy import create_engine
from pandas import DataFrame
from pathlib import Path
from arrow_space.input_layer import InputLayer
from arrow_space.input_layer import InputLayerCollection
from arrow_space.flattened_coordinate_dataset import create as create_arrowspace_dataset
from mojadata.util import gdal
from mojadata.util.gdal_calc import Calc
from mojadata.config import GDAL_CREATION_OPTIONS

class LayerConverterFactory:
    
    def __init__(self):
        self._temp_dir = TemporaryDirectory()
        self._converters = {
            "initial_current_land_class": self._convert_land_class,
            "initial_age": lambda layer: self._rename_layer(layer, "age"),
            "mean_annual_temperature": lambda layer: self._rename_layer(layer, "mean_annual_temp"),
            "inventory_delay": lambda layer: self._rename_layer(layer, "delay")
        }
    
    def convert(self, layer):
        converter = self._converters.get(layer.name)
        if converter:
            return converter(layer)
        
        return InputLayer(
            layer.name, str(layer.path),
            self._build_attribute_table(layer.tiler_metadata),
            layer.study_area_metadata.get("tags")
        )

    def _build_attribute_table(self, layer_metadata):
        gcbm_attribute_table = layer_metadata.get("attributes")
        if not gcbm_attribute_table:
            return None
        
        rows = []
        for att_id, att_value in gcbm_attribute_table.items():
            row = {"id": int(att_id)}
            if isinstance(att_value, dict):
                row.update({k: v for k, v in att_value.items() if k != "conditions"})
            else:
                row.update({"value": att_value})
            
            rows.append(row)

        return DataFrame(rows)

    def _rename_layer(self, layer, name):
        return InputLayer(
            name, str(layer.path),
            self._build_attribute_table(layer.tiler_metadata),
            layer.study_area_metadata.get("tags")
        )
    
    def _convert_land_class(self, layer):
        gcbm_cbm4_landclass_lookup = {
            "FL": 0,
            "CL": 1,
            "GL": 2,
            "WL": 3,
            "SL": 4,
            "OL": 5,
            "UFL": 16,
        }
        
        original_ndv = layer.tiler_metadata["nodata"]
        new_ndv = 32767

        px_calcs = (
            f"((A == {original_px}) * {gcbm_cbm4_landclass_lookup.get(gcbm_landclass, new_ndv)})"
            for original_px, gcbm_landclass in layer.tiler_metadata["attributes"].items()
        )

        calc = "+".join((
            f"((A == {original_ndv}) * {new_ndv})",
            *px_calcs
        ))

        output_path = Path(self._temp_dir.name).joinpath(f"{layer.name}.tif")
        Calc(calc, str(output_path), new_ndv, quiet=True, creation_options=GDAL_CREATION_OPTIONS,
             overwrite=True, hideNoData=False, type=gdal.GDT_Int16, A=layer.path)
        
        return InputLayer(
            "land_class", str(output_path),
            tags=layer.study_area_metadata.get("tags")
        )

class ProjectConverter:
    
    def __init__(self, layer_converter_factory=None):
        self._layer_converter_factory = layer_converter_factory or LayerConverterFactory()

    def convert(self, project, output_path):
        output_path = Path(output_path)
        shutil.rmtree(output_path, ignore_errors=True)
        output_path.mkdir(parents=True, exist_ok=True)
        
        self._convert_spatial_data(project, output_path)
        self._convert_yields(project, output_path)
        self._convert_transitions(project, output_path)

    @contextmanager
    def _input_db_connection(self, project):
        input_db_path = (
            project.rollback_db_path if project.has_rollback
            else project.input_db_path
        )
        
        connection_url = f"sqlite:///{input_db_path}"
        engine = create_engine(connection_url)
        with engine.connect() as conn:
            yield conn

    def _convert_spatial_data(self, project, output_path):
        arrowspace_layers = InputLayerCollection([
            self._layer_converter_factory.convert(layer)
            for layer in project.layers
        ])
            
        create_arrowspace_dataset(
            arrowspace_layers, "inventory", "local_storage",
            str(output_path.joinpath("inventory.arrowspace"))
        )

    def _flatten_pivot_columns(self, pivot_data):
        pivot_data.columns = [
            pivot_data.columns.get_level_values(1)[i] if pivot_data.columns.get_level_values(1)[i] != ""
            else pivot_data.columns.get_level_values(0)[i]
            for i in range(len(pivot_data.columns))
        ]

    def _convert_yields(self, project, output_path):
        with self._input_db_connection(project) as conn:
            components = pd.read_sql(
                """
                SELECT
                    gcc.id AS growth_curve_component_id, c.name AS classifier_name,
                    cv.value AS classifier_value
                FROM growth_curve_component gcc
                INNER JOIN growth_curve_classifier_value gccv
                    ON gcc.growth_curve_id = gccv.growth_curve_id
                INNER JOIN classifier_value cv
                    ON gccv.classifier_value_id = cv.id
                INNER JOIN classifier c
                    ON cv.classifier_id = c.id
                """, conn
            ).pivot(
                index="growth_curve_component_id", columns="classifier_name"
            ).reset_index().set_index("growth_curve_component_id")
            self._flatten_pivot_columns(components)

            component_species = pd.read_sql(
                """
                SELECT gcc.id AS growth_curve_component_id, s.name AS species
                FROM growth_curve_component gcc
                INNER JOIN species s
                    ON gcc.species_id = s.id
                """, conn
            ).set_index("growth_curve_component_id")

            component_values = pd.read_sql(
                """
                SELECT gcc.id AS growth_curve_component_id, gcv.age, gcv.merchantable_volume
                FROM growth_curve_component gcc
                INNER JOIN growth_curve_component_value gcv
                    ON gcc.id = gcv.growth_curve_component_id
                """, conn
            ).pivot(index="growth_curve_component_id", columns="age")
            self._flatten_pivot_columns(component_values)

            yield_output_path = output_path.joinpath("sit_yields.csv")
            yield_curves = components.join(component_species).join(component_values).reset_index()
            yield_curves.drop("growth_curve_component_id", axis=1).to_csv(yield_output_path, index=False)

    def _convert_transitions(self, project, output_path):
        with self._input_db_connection(project) as conn:
            transitions = pd.read_sql(
                """
                SELECT
                    t.id, t.regen_delay, t.age AS age_after,
                    c.name AS classifier_name, cv.value AS classifier_value
                FROM transition t
                INNER JOIN transition_classifier_value tcv
                    ON t.id = tcv.transition_id
                INNER JOIN classifier_value cv
                    ON tcv.classifier_value_id = cv.id
                INNER JOIN classifier c
                    ON cv.classifier_id = c.id
                """, conn
            ).pivot(index=["id", "regen_delay", "age_after"], columns="classifier_name").reset_index()
            self._flatten_pivot_columns(transitions)
            
            transition_output_path = output_path.joinpath("sit_transitions.csv")
            transitions.to_csv(transition_output_path, index=False)
