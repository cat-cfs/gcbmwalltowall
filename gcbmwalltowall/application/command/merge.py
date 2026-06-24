from __future__ import annotations
import logging
import shutil
from argparse import Namespace
from dataclasses import dataclass
from tempfile import TemporaryDirectory
from typing import Any
from psutil import virtual_memory
from spatial_inventory_rollback.gcbm.merge import gcbm_merge, gcbm_merge_tile
from spatial_inventory_rollback.gcbm.merge.gcbm_merge_input_db import (
    replace_direct_attached_transition_rules,
)
from gcbmwalltowall.component.preparedproject import PreparedProject
from gcbmwalltowall.configuration.configuration import Configuration
from gcbmwalltowall.configuration.gcbmconfigurer import GCBMConfigurer
from gcbmwalltowall.util.path import Path
from gcbmwalltowall.application.command.argbase import ArgBase


@dataclass
class MergeArgs(ArgBase):
    config_path: str
    project_paths: list[str]
    output_path: str
    include_index_layer: bool
    max_mem_gb: int
    tempdir: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]):
        return cls(
            config_path=d["config_path"],
            project_paths=d["project_paths"],
            output_path=d["output_path"],
            include_index_layer=d["include_index_layer"],
            max_mem_gb=d.get("max_mem_gb", None),
            tempdir=d.get("tempdir", None),
        )

    @classmethod
    def from_namespace(cls, ns: Namespace):
        return cls(
            config_path=ns.config_path,
            project_paths=ns.project_paths,
            output_path=ns.output_path,
            include_index_layer=ns.include_index_layer,
            max_mem_gb=getattr(ns, "max_mem_gb", None),
            tempdir=getattr(ns, "tempdir", None),
        )


def merge(args: MergeArgs | dict):
    args = args if isinstance(args, MergeArgs) else MergeArgs.from_dict(args)
    with TemporaryDirectory() as tmp:
        projects = [PreparedProject(path) for path in args.project_paths]
        logging.info(
            "Merging projects:\n{}".format("\n".join((str(p.path) for p in projects)))
        )
        inventories = [
            project.prepare_merge(tmp, i) for i, project in enumerate(projects)
        ]

        output_path = Path(args.output_path)
        merged_output_path = output_path.joinpath("layers", "merged")
        tiled_output_path = output_path.joinpath("layers", "tiled")
        db_output_path = output_path.joinpath("input_database")

        shutil.rmtree(merged_output_path, ignore_errors=True)

        start_year = min((project.start_year for project in projects))
        end_year = max((project.end_year for project in projects))

        max_mem_gb = args.max_mem_gb or (virtual_memory().available * 0.75 // 1024**3)
        memory_limit = int(max_mem_gb * 1024)
        merged_data = gcbm_merge.merge(
            inventories,
            str(merged_output_path),
            str(db_output_path),
            start_year,
            memory_limit_MB=memory_limit,
        )

        gcbm_merge_tile.tile(
            str(tiled_output_path), merged_data, inventories, args.include_index_layer
        )

        replace_direct_attached_transition_rules(
            str(db_output_path.joinpath("gcbm_input.db")),
            str(tiled_output_path.joinpath("transition_rules.csv")),
        )

        config = Configuration.load(args.config_path, args.output_path)
        configurer = GCBMConfigurer(
            [str(tiled_output_path)],
            config.gcbm_template_path,
            str(db_output_path.joinpath("gcbm_input.db")),
            str(output_path.joinpath("gcbm_project")),
            start_year,
            end_year,
            config.gcbm_disturbance_order,
        )

        configurer.configure()
