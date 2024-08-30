from __future__ import annotations
import logging
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
from pathlib import Path
from sqlalchemy import *
from arrow_space.input.raster_input_layer import RasterInputLayer
from arrow_space.input.raster_input_layer import RasterInputSource
from gcbmwalltowall.util.gdalhelpers import *
from gcbmwalltowall.util.rasterchunks import get_memory_limited_raster_chunks
from gcbmwalltowall.converter.layerconverter import LayerConverter

class LastPassDisturbanceLayerConverter(LayerConverter):
    
    def __init__(
        self,
        cbm_defaults_path: Path | str,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._cbm_defaults_path = Path(cbm_defaults_path)

    def handles(self, layer: PreparedLayer) -> bool:
        tags = layer.study_area_metadata.get("tags", [])
        return "last_pass_disturbance" in tags

    def convert_internal(self, layers: list[PreparedLayer]) -> list[RasterInputLayer]:
        if not layers:
            return []
        
        logging.info(f"Converting layers: {', '.join((l.name for l in layers))}")
        disturbance_info = [{"path": str(l.path), **l.metadata} for l in layers]
        first = disturbance_info[0]
        dimension = get_raster_dimension(first["path"])
        chunks = list(
            get_memory_limited_raster_chunks(
                n_rasters=2,
                height=dimension.y_size,
                width=dimension.x_size,
                memory_limit_MB=int(global_memory_limit / 1024**2 / max_threads)
            )
        )
        
        output_path = self._temp_dir.joinpath("last_pass_disturbance_type.tiff")
        create_empty_raster(
            first["path"],
            str(output_path),
            data_type=np.int32,
            options=gdal_creation_options,
            nodata=0
        )
        
        full_bound = get_raster_dimension(first["path"])
        last_past_disturbance = np.full(
            shape=(full_bound.y_size, full_bound.x_size),
            fill_value=0,
            dtype="int32"
        )
        
        disturbance_types = self._load_disturbance_types()

        # for each chunk, locate the most recent disturbance event
        with ProcessPoolExecutor() as pool:
            tasks = []
            for chunk in chunks:
                tasks.append(pool.submit(
                    self._process_chunk, disturbance_types, disturbance_info, chunk
                ))
                
            for task in as_completed(tasks):
                chunk, data = task.result()
                last_past_disturbance[
                    chunk.y_off : chunk.y_off + chunk.y_size,  # noqa: E203
                    chunk.x_off : chunk.x_off + chunk.x_size,  # noqa: E203
                ] = data

        write_output(str(output_path), last_past_disturbance, 0, 0)

        return [RasterInputLayer(output_path.stem, [RasterInputSource(path=str(output_path))])]

    def _process_chunk(
        self,
        disturbance_types: dict[str, int],
        disturbance_info: dict[str, Any],
        chunk: RasterBound
    ) -> NDArray:
        year = np.full((chunk.y_size * chunk.x_size), -1, "int32")
        disturbance_type = np.full((chunk.y_size * chunk.x_size), 0, "int32")
        for info in disturbance_info:
            ds = read_dataset(info["path"], bounds=chunk)
            info_year = np.full((chunk.y_size * chunk.x_size), -1, "int32")
            info_dist = np.full((chunk.y_size * chunk.x_size), 0, "int32")
            for att_key, att_value in info["attributes"].items():
                att_key_loc = np.where(ds.data.flatten() == int(att_key))[0]
                info_year[att_key_loc] = int(att_value["year"])
                info_dist[att_key_loc] = int(disturbance_types[att_value["disturbance_type"]])
                
            update_idx = np.where(info_year > year)[0]
            year[update_idx] = info_year[update_idx]
            disturbance_type[update_idx] = info_dist[update_idx]
            
        return chunk, disturbance_type.reshape((chunk.y_size, chunk.x_size))

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
    