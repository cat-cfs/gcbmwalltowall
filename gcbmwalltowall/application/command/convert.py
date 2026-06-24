from __future__ import annotations
import logging
from argparse import Namespace
from dataclasses import dataclass
from typing import Any
from gcbmwalltowall.component.preparedproject import PreparedProject
from gcbmwalltowall.application.command.argbase import ArgBase


@dataclass
class ConvertArgs(ArgBase):
    project_path: str
    output_path: str
    aidb_path: str
    spinup_disturbance_type: str
    preserve_temp_files: bool
    creation_options: dict
    max_workers: int
    chunk_size: int
    tempdir: str
    optimize_spinup: bool
    include_rollback_info: bool

    @classmethod
    def from_dict(cls, d: dict[str, Any]):
        return cls(
            project_path=d["project_path"],
            output_path=d["output_path"],
            aidb_path=d.get("aidb_path", None),
            spinup_disturbance_type=d.get("spinup_disturbance_type", "Wildfire"),
            preserve_temp_files=d.get("preserve_temp_files", False),
            creation_options=d.get("creation_options", {}),
            max_workers=d.get("max_workers", None),
            chunk_size=d.get("chunk_size", None),
            tempdir=d.get("tempdir", None),
            optimize_spinup=d.get("optimize_spinup", False),
            include_rollback_info=d.get("include_rollback_info", False),
        )

    @classmethod
    def from_namespace(cls, ns: Namespace):
        return cls(
            project_path=ns.project_path,
            output_path=ns.output_path,
            aidb_path=getattr(ns, "aidb_path", None),
            spinup_disturbance_type=getattr(ns, "spinup_disturbance_type", "Wildfire"),
            preserve_temp_files=getattr(ns, "preserve_temp_files", False),
            creation_options=getattr(ns, "creation_options", {}),
            max_workers=getattr(ns, "max_workers", None),
            chunk_size=getattr(ns, "chunk_size", None),
            tempdir=getattr(ns, "tempdir", None),
            optimize_spinup=getattr(ns, "optimize_spinup", False),
            include_rollback_info=getattr(ns, "include_rollback_info", False),
        )


def convert(args: ConvertArgs | dict):
    # Guard against importing CBM4 dependencies until needed.
    from gcbmwalltowall.converter.projectconverter import ProjectConverter

    args = args if isinstance(args, ConvertArgs) else ConvertArgs.from_dict(args)
    creation_options = args.creation_options or {}
    creation_options["max_workers"] = args.max_workers
    chunk_size = args.chunk_size
    if chunk_size:
        creation_options.update(
            {
                "chunk_options": {
                    "chunk_x_size_max": chunk_size,
                    "chunk_y_size_max": chunk_size,
                }
            }
        )

    project = PreparedProject(args.project_path, args.include_rollback_info)
    logging.info(f"Converting {project.path} to CBM4")
    converter = ProjectConverter(creation_options)
    converter.convert(
        project,
        args.output_path,
        args.aidb_path,
        args.spinup_disturbance_type,
        args.preserve_temp_files,
        args.optimize_spinup
    )
