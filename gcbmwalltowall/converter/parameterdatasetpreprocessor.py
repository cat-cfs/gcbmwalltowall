import duckdb
import pandas as pd
from functools import cache
from pathlib import Path
from collections import defaultdict
from arrow_space import dataset_common
from arrow_space.raster_indexed_dataset import RasterIndexedDataset
from arrow_space.flattened_coordinate_dataset import FlattenedCoordinateDataset
from cbm4.app.spatial.gcbm_input.timestep_interpreter import TimestepInterpreter


class ParameterDatasetPreprocessor:

    def __init__(
        self,
        timestep_interpreter: TimestepInterpreter,
        cbm_defaults_path: str | Path,
    ):
        self._timestep_interpreter = timestep_interpreter
        self._cbm_defaults_path = str(cbm_defaults_path)

    def preprocess(
        self,
        input_dataset_config: dict[str, str],
        output_dataset_config: dict[str, str]
    ):
        input_ds = FlattenedCoordinateDataset(**input_dataset_config)
        dataset_common.create_empty(
            output_dataset_config["dataset_name"],
            output_dataset_config["storage_type"],
            output_dataset_config["storage_path_or_uri"],
            input_ds.chunks,
            {
                "chunk_index": "int32",
                "timestep": "int32",
            },
            input_ds.get_geo_metadata(),
        )

        output_ds = RasterIndexedDataset(**output_dataset_config)

        attribute_tables = self._get_attribute_tables(input_ds)
        timesteps = pd.concat(attribute_tables.values())["timestep"].unique()
        for partition in input_ds.get_partition_values():
            partition_filter = [(k, "=", v) for k, v in partition.items()]
            partition_data = defaultdict(list)
            for layer in input_ds.get_layer_names():
                timeseries_tag = next(t for t in input_ds.get_tags(layer) if "timeseries" in t)
                timeseries_name = timeseries_tag.split(":")[1]
                partition_data[timeseries_name].append(
                    pd.merge(
                        input_ds.read_pandas(
                            filters=partition_filter,
                            read_cols=["chunk_index", "raster_index", layer],
                        ),
                        attribute_tables[layer],
                        left_on=layer,
                        right_on="id",
                    )[["chunk_index", "raster_index", "timestep", "value"]].rename(
                        columns={"value": timeseries_name}
                    )
                )

            joined_data = None
            for timeseries_data in partition_data.values():
                timeseries_data = pd.concat(timeseries_data).drop_duplicates(
                    subset=["timestep", "chunk_index", "raster_index"]
                )

                if joined_data is None:
                    joined_data = timeseries_data
                else:
                    joined_data = pd.merge(
                        joined_data,
                        timeseries_data,
                        how="outer",
                        on=["timestep", "chunk_index", "raster_index"]
                    )

            for timestep in timesteps:
                out_data, out_data_raster_index = output_ds.as_raster_indexed(
                    joined_data[joined_data["timestep"] == timestep]
                )

                out_data = self._ensure_correct_data_types(out_data)
                out_data_raster_index["timestep"] = timestep
                output_ds.write(out_data)
                output_ds.write_raster_index(out_data_raster_index)

    def _get_attribute_tables(self, ds: FlattenedCoordinateDataset):
        attribute_tables = {
            layer_name: ds.get_attributes(layer_name)
            for layer_name in ds.get_layer_names()
        }

        for attribute_table in attribute_tables.values():
            attribute_table["timestep"] = attribute_table.astype(
                {"year": "int"}
            ).apply(self._timestep_interpreter.get_timestep, axis=1)

        return attribute_tables

    def _ensure_correct_data_types(self, parameter_data: pd.DataFrame):
        data_types = self._get_parameter_data_types()
        
        return parameter_data.astype({
            c: data_types.get(c, "str") for c in parameter_data.columns
        })

    @cache
    def _get_parameter_data_types(self):
        cbm_defaults = duckdb.connect(self._cbm_defaults_path)
        dtypes = cbm_defaults.query(
            """
            SELECT DISTINCT
                column_name,
                CASE
                    WHEN data_type = 'BIGINT' THEN 'int64'
                    WHEN data_type = 'BOOLEAN' THEN 'bool'
                    WHEN data_type = 'DOUBLE' THEN 'double'
                    WHEN data_type = 'FLOAT' THEN 'float'
                    WHEN data_type = 'INTEGER' THEN 'int32'
                    WHEN data_type = 'VARCHAR' THEN 'str'
                    WHEN data_type = 'TEXT' THEN 'str'
                    ELSE 'str'
                END AS pandas_dtype
            FROM DUCKDB_COLUMNS()
            WHERE database_name <> 'system'
            """
        ).df()

        type_map = {
            row["column_name"]: row["pandas_dtype"]
            for _, row in dtypes.iterrows()
        }

        type_map["index"] = "int64"

        return type_map
