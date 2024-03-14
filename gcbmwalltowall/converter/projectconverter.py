import shutil
import json
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from pandas import DataFrame
from pathlib import Path
from arrow_space.input_layer import InputLayer
from arrow_space.input_layer import InputLayerCollection
from arrow_space.flattened_coordinate_dataset import create as create_arrowspace_dataset

class ProjectConverter:
    
    def __init__(self):
        pass

    def convert(self, project, output_path):
        output_path = Path(output_path)
        shutil.rmtree(output_path, ignore_errors=True)
        output_path.mkdir(parents=True, exist_ok=True)
        
        self._convert_spatial_data(project, output_path)
        self._convert_yields(project, output_path)

        transition_rules_path = (
            project.tiled_layer_path.joinpath("transition_rules.csv") if not project.has_rollback
            else project.rollback_layer_path.joinpath("transition_rules.csv")
        )
        
        if transition_rules_path.exists():
            shutil.copyfile(transition_rules_path, output_path.joinpath("transition_rules.csv"))

    def _convert_spatial_data(self, project, output_path):
        arrowspace_layers = InputLayerCollection([
            InputLayer(
                layer.name, str(layer.path),
                self._build_attribute_table(layer.tiler_metadata),
                layer.study_area_metadata.get("tags")
            ) for layer in project.layers
        ])
            
        create_arrowspace_dataset(
            arrowspace_layers, "inventory", "local_storage",
            str(output_path.joinpath("inventory.arrowspace"))
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

    def _convert_yields(self, project, output_path):
        input_db_path = (
            project.rollback_db_path if project.has_rollback
            else project.input_db_path
        )
        
        connection_url = f"sqlite:///{input_db_path}"
        engine = create_engine(connection_url)
        with engine.connect() as conn:
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

    def _flatten_pivot_columns(self, pivot_data):
        pivot_data.columns = [
            pivot_data.columns.get_level_values(1)[i] if pivot_data.columns.get_level_values(1)[i] != ""
            else pivot_data.columns.get_level_values(0)[i]
            for i in range(len(pivot_data.columns))
        ]
