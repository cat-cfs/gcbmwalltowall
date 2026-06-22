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

    @classmethod
    def from_dict(cls, d: dict[str, Any]):
        return cls(
            cbm4_config_path=d["config_path"],
            output_path=d["output_path"],
            start_year=d.get("start_year", None),
            end_year=d.get("end_year", None),
            include_disturbances=d.get("include_disturbances", False),
        )

    @classmethod
    def from_namespace(cls, ns: Namespace):
        return cls(
            cbm4_config_path=ns.cbm4_config_path,
            output_path=ns.output_path,
            start_year=getattr(ns, "start_year", None),
            end_year=getattr(ns, "end_year", None),
            include_disturbances=getattr(ns, "include_disturbances", False),
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

    clone_cbm4_config_path = config.resolve_working("cbm4_config.json")
    shutil.copyfile(args.cbm4_config_path, clone_cbm4_config_path)
    with GCBMConfigurer.update_json_file(clone_cbm4_config_path) as cbm4_config:
        cbm4_config["cbm4_spatial_dataset"]["inventory"]["path_or_uri"] = "inventory"
        cbm4_config["cbm4_spatial_dataset"]["disturbance"]["path_or_uri"] = "disturbance"
        cbm4_config["cbm4_spatial_dataset"]["simulation"]["path_or_uri"] = "simulation"
        cbm4_config["cache"] = {
            "dataset_name": "simulation",
            "storage_type": "local_storage",
            "path_or_uri": os.path.relpath(
                config.resolve(config["cbm4_spatial_dataset"]["simulation"]["path_or_uri"]),
                clone_cbm4_config_path.parent
            ),
            "end_year": (
                args["start_year"] - 1 if "start_year" in args
                else config["end_year"]
            )
        }

        if args.end_year is not None:
            cbm4_config["end_year"] = args.end_year
