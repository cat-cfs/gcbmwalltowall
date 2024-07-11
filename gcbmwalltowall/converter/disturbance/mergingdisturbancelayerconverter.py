from __future__ import annotations
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
from sqlalchemy import *
from arrow_space.input.gdal_input_layer import GdalInputLayer
from gcbmwalltowall.util.gdalhelpers import *
from gcbmwalltowall.util.rasterchunks import get_memory_limited_raster_chunks
from gcbmwalltowall.util.numba import numba_map
from gcbmwalltowall.converter.layerconverter import LayerConverter
from gcbmwalltowall.converter.disturbance.compositedisturbancetypemanager import CompositeDisturbanceTypeManager
from gcbmwalltowall.converter.disturbance.eventmerger import EventMerger

class MergingDisturbanceLayerConverter(LayerConverter):
    
    def __init__(
        self,
        cbm_defaults_path: Path | str,
        sim_start_year: int,
        *args,
        disturbance_order: list[str] = None,
        excluded_disturbances: list[str] = None,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._cbm_defaults_path = Path(cbm_defaults_path)
        self._sim_start_year = sim_start_year
        self._disturbance_order = disturbance_order
        self._excluded_disturbances = excluded_disturbances or []

    def handles(self, layer: PreparedLayer) -> bool:
        tags = layer.study_area_metadata.get("tags", [])
        return "disturbance" in tags and "last_pass_disturbance" not in tags

    def convert_internal(self, layers: list[PreparedLayer]) -> list[GdalInputLayer]:
        if not layers:
            return []

        logging.info(f"Converting layers: {', '.join((l.name for l in layers))}")
        disturbance_type_order = self._load_disturbance_type_order()
        disturbance_info = [{"path": str(l.path), **l.metadata} for l in layers]
        first = disturbance_info[0]
        db_disturbance_types = self._load_disturbance_types()
        self._assert_known_dist_types(db_disturbance_types, self._excluded_disturbances)

        # gather all years in the disturbance layers
        years = set()
        dist_type_names = set()
        for info in disturbance_info:
            for _, att_value in info["attributes"].items():
                year = int(att_value["year"])
                if year < self._sim_start_year:
                    continue
                
                years.add(year)
                dist_type_names.add(str(att_value["disturbance_type"]))

        self._assert_known_dist_types(db_disturbance_types, dist_type_names)
        dist_type_lookup: dict[str, int] = {}
        for name in dist_type_names:
            dist_type_lookup[name] = db_disturbance_types[name]

        output_raster_paths = {}
        for year in years:
            raster_path = self._temp_dir.joinpath(f"{year}_disturbances.tiff")
            raster_path.unlink(True)
            output_raster_paths[year] = raster_path
            create_empty_raster(
                first["path"],
                output_raster_paths[year],
                data_type=np.int32,
                options=gdal_creation_options,
                nodata=0
            )

        dimension = get_raster_dimension(first["path"])

        disturbance_type_sort = {
            dist_type_lookup[name]: disturbance_type_order[name]
            for name in dist_type_lookup.keys()
        }
        
        composite_dist_type_manager = CompositeDisturbanceTypeManager(
            self._cbm_defaults_path, 1
        )
        
        for year in years:
            year_filtered_info = self._get_year_filtered_disturbance_info(year, disturbance_info)
            chunks = list(
                get_memory_limited_raster_chunks(
                    n_rasters=len(year_filtered_info),
                    height=dimension.y_size,
                    width=dimension.x_size,
                    memory_limit_MB=int(gdal_memory_limit / 1024**2),
                )
            )
            
            composite_dist_types = composite_dist_type_manager.get_all_composite_types()
            merger = EventMerger(
                dimension,
                disturbance_type_sort,
                composite_dist_type_manager.get_max_dist_type_id() + 1,
                keep_duplicates=True,
                composite_dist_types=composite_dist_types
            )
            
            with ProcessPoolExecutor() as pool:
                tasks = []
                for chunk in chunks:
                    tasks.append(pool.submit(
                        self._process_chunk, chunk, year, year_filtered_info, dist_type_lookup
                    ))
                
                for task in as_completed(tasks):
                    result = task.result()
                    if result is not None:
                        chunk, merge_data = result
                        merger.merge(chunk, merge_data)

            write_output(output_raster_paths[year], merger.merged_layer, 0, 0)
            for dist, members in merger.composite_dist_types_by_type.items():
                composite_dist_type_manager.add_composite_type(dist, members)

        return [
            GdalInputLayer(
                f"disturbances_{year}",
                str(layer_path),
                tags=["disturbance"]
            ) for year, layer_path in output_raster_paths.items()
        ]
    
    def _process_chunk(self, chunk, year, year_filtered_info, dist_type_lookup):
        disturbance_layers: dict[str, GDALHelperDataset] = {}
        for info in year_filtered_info:
            disturbance_layers[info["path"]] = read_dataset(
                info["path"], bounds=chunk
            )
                
        for info in year_filtered_info:
            dist_type_mapping = {}
            for att_key, att_value in info["attributes"].items():
                att_key = np.float32(att_key)
                default_dist_type_id = dist_type_lookup[att_value["disturbance_type"]]
                if att_value["year"] != year:
                    dist_type_mapping[att_key] = np.float32(0)
                elif att_value["disturbance_type"] in self._excluded_disturbances:
                    dist_type_mapping[att_key] = np.float32(0)
                else:
                    dist_type_mapping[att_key] = np.float32(default_dist_type_id)

            if dist_type_mapping:
                ds = disturbance_layers[info["path"]]
                dist_type_mapping[np.float32(ds.nodata)] = np.float32(0)
                merge_data = numba_map(
                    ds.data.astype("float32"), dist_type_mapping
                )
                        
                merge_data = merge_data.astype("int32")
                
                return chunk, merge_data

    def _load_disturbance_type_order(self) -> dict[str, int]:
        db_dist_types_df = pd.DataFrame(self._load_disturbance_types().keys(), columns=["name"])
        if self._disturbance_order:
            # ensure no duplicates in the user disturbance type order
            unique_user_dist_types = set(self._disturbance_order)
            if not len(unique_user_dist_types) == len(self._disturbance_order):
                raise ValueError(f"duplicate values detected in user disturbance type order")
            
            # check that every disturbance type in the user order exists in the database
            unknown_disturbance_types = unique_user_dist_types.difference(
                set(db_dist_types_df["name"].unique())
            )
            
            if unknown_disturbance_types:
                raise ValueError(
                    "entries in user disturbance type order not found in database"
                    f" {self._cbm_defaults_path}: {unknown_disturbance_types}"
                )
        
            output_order = self._disturbance_order
            for name in db_dist_types_df["name"]:
                if name not in unique_user_dist_types:
                    output_order.append(name)
        else:
            output_order = list(db_dist_types_df["name"])

        return {name: i for i, name in enumerate(output_order)}

    def _load_disturbance_types(self) -> dict:
        engine = create_engine(f"sqlite:///{self._cbm_defaults_path}")
        with engine.connect() as conn:
            dist_types = pd.read_sql_query(
                """
                SELECT disturbance_type_id, name
                FROM disturbance_type_tr
                WHERE locale_id = 1
                ORDER BY disturbance_type_id
                """,
                conn
            )

        return {
            str(row["name"]): int(row["disturbance_type_id"])
            for _, row in dist_types.iterrows()
        }

    def _assert_known_dist_types(self, db_dist_types, other_dist_types):
        missing_dist_types = set(other_dist_types).difference(set(db_dist_types))
        if missing_dist_types:
            raise ValueError(
                f"specified disturbance types ({missing_dist_types}) are not present in database"
            )

    def _get_year_filtered_disturbance_info(self, year: int, disturbance_info: list) -> list:
        filtered_info = [info for info in disturbance_info if any(
            (year == int(att_value["year"]) for att_value in info["attributes"].values())
        )]

        return filtered_info
