from __future__ import annotations
import os
import shutil
from argparse import Namespace
from dataclasses import dataclass
from typing import Any
from gcbmwalltowall.configuration.configuration import Configuration
from gcbmwalltowall.application.command.argbase import ArgBase
from gcbmwalltowall.configuration.gcbmconfigurer import GCBMConfigurer


@dataclass
class CloneArgs(ArgBase):
    cbm4_config_path: str
    output_path: str
    start_year: int
    end_year: int
    include_disturbances: bool
    use_cache: bool

    @classmethod
    def from_dict(cls, d: dict[str, Any]):
        return cls(
            cbm4_config_path=d["config_path"],
            output_path=d["output_path"],
            start_year=d.get("start_year", None),
            end_year=d.get("end_year", None),
            include_disturbances=d.get("include_disturbances", False),
            use_cache=d.get("use_cache", True),
        )

    @classmethod
    def from_namespace(cls, ns: Namespace):
        return cls(
            cbm4_config_path=ns.cbm4_config_path,
            output_path=ns.output_path,
            start_year=getattr(ns, "start_year", None),
            end_year=getattr(ns, "end_year", None),
            include_disturbances=getattr(ns, "include_disturbances", False),
            use_cache=getattr(ns, "use_cache", True),
        )


def clone(args: CloneArgs | dict):
    from arrow_space.raster_indexed_dataset import RasterIndexedDataset

    args = args if isinstance(args, CloneArgs) else CloneArgs.from_dict(args)
    shutil.rmtree(args.output_path, True)
    config = Configuration.load(args.cbm4_config_path, args.output_path)

    inventory = RasterIndexedDataset(
        config["cbm4_spatial_dataset"]["inventory"]["dataset_name"],
        config["cbm4_spatial_dataset"]["inventory"]["storage_type"],
        str(config.resolve(config["cbm4_spatial_dataset"]["inventory"]["path_or_uri"])),
    )

    clone_inventory_path = config.resolve_working("inventory")
    inventory.copy("inventory", "local_storage", str(clone_inventory_path))

    disturbance = RasterIndexedDataset(
        config["cbm4_spatial_dataset"]["disturbance"]["dataset_name"],
        config["cbm4_spatial_dataset"]["disturbance"]["storage_type"],
        str(config.resolve(config["cbm4_spatial_dataset"]["disturbance"]["path_or_uri"])),
    )

    clone_disturbance_path = config.resolve_working("disturbance")
    if args.include_disturbances:
        disturbance.copy("disturbance", "local_storage", str(clone_disturbance_path))
    else:
        disturbance.create_new(
            "disturbance",
            "local_storage",
            str(clone_disturbance_path),
            copy_raster_index_data=False,
        )

    for project_file in config.config_path.glob("*.*"):
        shutil.copyfile(project_file, config.resolve_working(project_file.name))

    clone_cbm4_config_path = config.resolve_working("cbm4_config.json")
    shutil.copyfile(args.cbm4_config_path, clone_cbm4_config_path)
    with GCBMConfigurer.update_json_file(clone_cbm4_config_path) as cbm4_config:
        cbm4_config["cbm4_spatial_dataset"]["inventory"]["path_or_uri"] = "inventory"
        cbm4_config["cbm4_spatial_dataset"]["disturbance"]["path_or_uri"] = "disturbance"
        cbm4_config["cbm4_spatial_dataset"]["simulation"]["path_or_uri"] = "simulation"
        if args.use_cache:
            # Cache rules: if the start_year is explicitly specified and is within the
            # parent project's simulation period, use the parent project as the cache,
            # otherwise use the parent project's cache if available.
            cache_end_year = (
                (args["start_year"] - 1) if args.get("start_year") is not None
                else config["end_year"]
            )

            parent_cache_config = config.get("cache")
            use_previous_cache = (
                (cache_end_year <= parent_cache_config["end_year"]) if parent_cache_config
                else False
            )

            cache_path = (
                os.path.relpath(
                    config.resolve(parent_cache_config["path_or_uri"]),
                    clone_cbm4_config_path.parent
                ) if use_previous_cache
                else os.path.relpath(
                    config.resolve(config["cbm4_spatial_dataset"]["simulation"]["path_or_uri"]),
                    clone_cbm4_config_path.parent
                )
            )

            cbm4_config["cache"] = {
                "dataset_name": "simulation",
                "storage_type": "local_storage",
                "path_or_uri": cache_path,
                "end_year": cache_end_year,
            }
        else:
            cbm4_config.pop("cache", None)

        if args.end_year is not None:
            cbm4_config["end_year"] = args.end_year
