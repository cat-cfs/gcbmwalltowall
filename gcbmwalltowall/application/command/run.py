from __future__ import annotations
import logging
import subprocess
import sys
from argparse import Namespace
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from gcbmwalltowall.component.preparedproject import PreparedProject
from gcbmwalltowall.configuration.configuration import Configuration
from gcbmwalltowall.util.path import Path
from gcbmwalltowall.application.command.argbase import ArgBase


@dataclass
class RunArgs(ArgBase):
    host: str
    project_path: str
    config_path: str
    end_year: int
    title: str
    compile_results_config: str
    batch_limit: int
    max_workers: int
    engine: str
    write_parameters: bool

    @classmethod
    def from_dict(cls, d: dict[str, Any]):
        return cls(
            project_path=d["project_path"],
            host=d.get("host", "local"),
            config_path=d.get("config_path", None),
            end_year=d.get("end_year", None),
            title=d.get("title", None),
            compile_results_config=d.get("compile_results_config", None),
            batch_limit=d.get("batch_limit", None),
            max_workers=d.get("max_workers", None),
            engine=d.get("engine", "libcbm"),
            write_parameters=d.get("write_parameters", False),
        )

    @classmethod
    def from_namespace(cls, ns: Namespace):
        return cls(
            project_path=ns.project_path,
            host=getattr(ns, "host", "local"),
            config_path=getattr(ns, "config_path", None),
            end_year=getattr(ns, "end_year", None),
            title=getattr(ns, "title", None),
            compile_results_config=getattr(ns, "compile_results_config", None),
            batch_limit=getattr(ns, "batch_limit", None),
            max_workers=getattr(ns, "max_workers", None),
            engine=getattr(ns, "engine", "libcbm"),
            write_parameters=getattr(ns, "write_parameters", False),
        )


def run(args: RunArgs | dict):
    args = args if isinstance(args, RunArgs) else RunArgs.from_dict(args)
    project = PreparedProject(args.project_path)
    run_type = "Queueing" if args.host == "cluster" else "Running"
    logging.info(f"{run_type} project ({args.host}):\n{project.path}")

    with project.temporary_new_end_year(args.end_year):
        config = (
            Configuration.load(args.config_path, args.project_path)
            if args.config_path
            else Configuration({}, "")
        )

        if args.host == "local":
            cbm4_config_path = Path(args.project_path).joinpath("cbm4_config.json")
            if cbm4_config_path.exists():
                extra_kwargs: dict[str, Any] = dict()

                match args.engine:
                    case "libcbm":
                        from gcbmwalltowall.runner import cbm4
                    case "cbmspec":
                        from gcbmwalltowall.runner import cbmspec as cbm4
                    case "canfire":
                        from gcbmwalltowall.runner import canfire as cbm4

                        model = cbm4.get_single_matrix_cbmspec(cbm4_config_path)
                        extra_kwargs["wrapped_cbmspec_model"] = model

                    case _:
                        raise RuntimeError(f"Unrecognized CBM4 engine: {args.engine}")

                cbm4.run(
                    str(cbm4_config_path),
                    max_workers=args.max_workers,
                    write_parameters=args.write_parameters,
                    end_year=args.end_year,
                    **extra_kwargs,
                )
            else:
                logging.info(f"Using {config.resolve(config.gcbm_exe)}")
                subprocess.run(
                    [
                        str(config.resolve(config.gcbm_exe)),
                        "--config_file",
                        "gcbm_config.cfg",
                        "--config_provider",
                        "provider_config.json",
                    ],
                    cwd=project.gcbm_config_path,
                )
        elif args.host == "cluster":
            logging.info(f"Using {config.resolve(config.distributed_client)}")
            project_name = config.get("project_name", project.path.stem)

            run_args = [
                sys.executable,
                str(config.resolve(config.distributed_client)),
                "--title",
                datetime.now().strftime(
                    f"gcbm_{args.title or project_name}_%Y%m%d_%H%M%S"
                ),
                "--gcbm-config",
                str(project.gcbm_config_path.joinpath("gcbm_config.cfg")),
                "--provider-config",
                str(project.gcbm_config_path.joinpath("provider_config.json")),
                "--study-area",
                str(
                    (project.rollback_layer_path or project.tiled_layer_path).joinpath(
                        "study_area.json"
                    )
                ),
                "--no-wait",
            ]

            compile_results_config = args.compile_results_config
            if compile_results_config:
                run_args.extend(
                    [
                        "--compile-results-config",
                        Path(compile_results_config).absolute(),
                    ]
                )

            batch_limit = args.batch_limit
            if batch_limit:
                run_args.extend(["--batch-limit", batch_limit])

            subprocess.run(run_args, cwd=project.path)

    logging.info(f"Finished {run_type.lower()} project ({args.host}):\n{project.path}")
