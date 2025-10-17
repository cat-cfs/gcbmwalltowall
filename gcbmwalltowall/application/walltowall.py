from __future__ import annotations

import logging
import multiprocessing as mp
import shutil
import subprocess
import sys
from argparse import ArgumentParser, Namespace
from dataclasses import dataclass
from datetime import datetime
from logging import FileHandler, StreamHandler
from tempfile import TemporaryDirectory
from typing import Any

from psutil import virtual_memory
from spatial_inventory_rollback.gcbm.merge import gcbm_merge, gcbm_merge_tile
from spatial_inventory_rollback.gcbm.merge.gcbm_merge_input_db import (
    replace_direct_attached_transition_rules,
)

from gcbmwalltowall.builder.projectbuilder import ProjectBuilder
from gcbmwalltowall.component.preparedproject import PreparedProject
from gcbmwalltowall.configuration.configuration import Configuration
from gcbmwalltowall.configuration.gcbmconfigurer import GCBMConfigurer
from gcbmwalltowall.project.projectfactory import ProjectFactory
from gcbmwalltowall.util.path import Path


class ArgBase(dict):

    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, value):
        self[key] = value


@dataclass
class ConvertArgs(ArgBase):
    project_path: str
    output_path: str
    aidb_path: str
    spinup_disturbance_type: str
    apply_departial_dms: bool
    preserve_temp_files: bool
    creation_options: dict
    max_workers: int
    chunk_size: int
    tempdir: str

    @classmethod
    def from_namespace(cls, ns: Namespace):
        return cls(
            project_path=ns.project_path,
            output_path=ns.output_path,
            aidb_path=getattr(ns, "aidb_path", None),
            spinup_disturbance_type=getattr(ns, "spinup_disturbance_type", "Wildfire"),
            apply_departial_dms=getattr(ns, "apply_departial_dms", False),
            preserve_temp_files=getattr(ns, "preserve_temp_files", False),
            creation_options=getattr(ns, "creation_options", {}),
            max_workers=getattr(ns, "max_workers", None),
            chunk_size=getattr(ns, "chunk_size", None),
            tempdir=getattr(ns, "tempdir", None),
        )


@dataclass
class BuildArgs(ArgBase):
    config_path: str
    output_path: str

    @classmethod
    def from_namespace(cls, ns: Namespace):
        return cls(
            config_path=ns.config_path,
            output_path=ns.output_path,
        )


@dataclass
class PrepareArgs(ArgBase):
    config_path: str
    output_path: str
    max_workers: int
    max_mem_gb: int

    @classmethod
    def from_namespace(cls, ns: Namespace):
        return cls(
            config_path=ns.config_path,
            output_path=getattr(ns, "output_path", None),
            max_workers=getattr(ns, "max_workers", None),
            max_mem_gb=getattr(ns, "max_mem_gb", None),
        )


@dataclass
class MergeArgs(ArgBase):
    config_path: str
    project_paths: list[str]
    output_path: str
    include_index_layer: bool
    max_mem_gb: int
    tempdir: str

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


def convert(args: ConvertArgs | dict):
    # Guard against importing CBM4 dependencies until needed.
    from gcbmwalltowall.converter.projectconverter import ProjectConverter

    args = ConvertArgs(**args)
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

    project = PreparedProject(args.project_path)
    logging.info(f"Converting {project.path} to CBM4")
    converter = ProjectConverter(creation_options)
    converter.convert(
        project,
        args.output_path,
        args.aidb_path,
        args.spinup_disturbance_type,
        args.apply_departial_dms,
        args.preserve_temp_files,
    )


def _convert(args: Namespace):
    convert(ConvertArgs.from_namespace(args))


def build(args: BuildArgs | dict):
    args = BuildArgs(**args)
    logging.info(f"Building {args.config_path}")
    ProjectBuilder.build_from_file(args.config_path, args.output_path)


def _build(args: Namespace):
    build(BuildArgs.from_namespace(args))


def prepare(args: PrepareArgs | dict):
    args = PrepareArgs(**args)
    config = Configuration.load(args.config_path, args.output_path)
    config["max_workers"] = args.max_workers
    config["max_mem_gb"] = args.max_mem_gb
    project = ProjectFactory().create(config)
    logging.info(f"Preparing {project.name}")

    project.tile()
    project.create_input_database()
    project.run_rollback()

    extra_args = {
        param: config.get(param)
        for param in ("start_year", "end_year")
        if config.get(param)
    }

    project.configure_gcbm(
        config.gcbm_template_path, config.gcbm_disturbance_order, **extra_args
    )


def _prepare(args: Namespace):
    prepare(PrepareArgs.from_namespace(args))


def merge(args: MergeArgs | dict):
    args = MergeArgs(**args)
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


def _merge(args: Namespace):
    merge(MergeArgs.from_namespace(args))


def run(args: RunArgs | dict):
    args = RunArgs(**args)
    project = PreparedProject(args.project_path)
    logging.info(f"Running project ({args.host}):\n{project.path}")

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


def _run(args: RunArgs):
    run(RunArgs.from_namespace(args))


def cli():
    try:
        mp.set_start_method("spawn")
    except:
        pass

    parser = ArgumentParser(description="Manage GCBM wall-to-wall projects")
    parser.set_defaults(func=lambda _: parser.print_help())
    subparsers = parser.add_subparsers(help="Command to run")

    build_parser = subparsers.add_parser(
        "build",
        help=(
            "Use the builder configuration contained in the config file to fill in and "
            "configure the rest of the project; overwrites the existing json config file "
            "unless output config file path is specified."
        ),
    )
    build_parser.set_defaults(func=_build)
    build_parser.add_argument(
        "config_path", help="path to config file containing shortcut 'builder' section"
    )
    build_parser.add_argument(
        "output_path", nargs="?", help="destination directory for build output"
    )

    prepare_parser = subparsers.add_parser(
        "prepare",
        help=(
            "Using the project configuration in the config file, tile the spatial "
            "layers, generate the input database, run the spatial rollback if "
            "specified, and configure the GCBM run."
        ),
    )
    prepare_parser.set_defaults(func=_prepare)
    prepare_parser.add_argument(
        "config_path",
        help="path to config file containing fully-specified project configuration",
    )
    prepare_parser.add_argument(
        "output_path", nargs="?", help="destination directory for project files"
    )
    prepare_parser.add_argument("--max_workers", type=int, help="max workers")
    prepare_parser.add_argument("--max_mem_gb", type=int, help="max memory (GB)")

    merge_parser = subparsers.add_parser(
        "merge", help="Merge two or more walltowall-prepared inventories together."
    )
    merge_parser.set_defaults(func=_merge, include_index_layer=False)
    merge_parser.add_argument(
        "config_path",
        help="path to walltowall config file for disturbance order and GCBM config templates",
    )
    merge_parser.add_argument(
        "project_paths",
        nargs="+",
        help="root directories of at least two walltowall-prepared projects",
    )
    merge_parser.add_argument(
        "--output_path", required=True, help="path to generate merged output in"
    )
    merge_parser.add_argument(
        "--include_index_layer",
        action="store_true",
        help="include merged index as reporting classifier",
    )
    merge_parser.add_argument("--max_mem_gb", type=int, help="max memory (GB)")

    run_parser = subparsers.add_parser(
        "run", help="Run the specified project either locally or on the cluster."
    )
    run_parser.set_defaults(func=_run)
    run_parser.add_argument(
        "host",
        choices=["local", "cluster"],
        help="run either locally or on the cluster",
    )
    run_parser.add_argument(
        "project_path", help="root directory of the walltowall-prepared project to run"
    )
    run_parser.add_argument(
        "--config_path",
        help="path to config file containing fully-specified project configuration",
    )
    run_parser.add_argument(
        "--end_year", type=int, help="temporarily set a new end year for this run"
    )
    run_parser.add_argument("--title", help="explicitly specify a title for this run")
    run_parser.add_argument(
        "--compile_results_config", help="path to custom compile results config file"
    )
    run_parser.add_argument(
        "--batch_limit", help="[cluster only] batch limit for cluster runs"
    )
    run_parser.add_argument(
        "--max_workers", type=int, help="[cbm4 only] max workers for CBM4 runs"
    )
    run_parser.add_argument(
        "--engine",
        help="[cbm4 only] (libcbm/cbmspec) specify the CBM4 engine to use; default: libcbm",
        default="libcbm",
    )
    run_parser.add_argument(
        "--write_parameters",
        action="store_true",
        help="[cbm4 only] write parameters datasets; default: false",
    )

    convert_parser = subparsers.add_parser(
        "convert", help=("Convert a walltowall-prepared GCBM project to CBM4.")
    )
    convert_parser.set_defaults(func=_convert, creation_options={})
    convert_parser.add_argument(
        "project_path", help="root directory of a walltowall-prepared GCBM project"
    )
    convert_parser.add_argument(
        "output_path", help="destination directory for CBM4 project files"
    )
    convert_parser.add_argument(
        "--aidb_path", help="AIDB to use when building CBM4 input database"
    )
    convert_parser.add_argument("--chunk_size", help="maximum CBM4 chunk size")
    convert_parser.add_argument(
        "--spinup_disturbance_type", help="override default spinup disturbance type"
    )
    convert_parser.add_argument(
        "--apply_departial_dms",
        action="store_true",
        help="apply departial DMs (cohorts)",
    )
    convert_parser.add_argument(
        "--max_workers", type=int, help="max workers for CBM4 conversion"
    )
    convert_parser.add_argument(
        "--preserve_temp_files",
        action="store_true",
        help="preserve temporary files generated during conversion",
    )

    args = parser.parse_args()

    log_path = Path(
        args.output_path
        if getattr(args, "output_path", None)
        else args.project_path if getattr(args, "project_path", None) else "."
    ).joinpath("walltowall.log")

    log_path.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        handlers=[
            FileHandler(log_path, mode=("a" if args.func == run else "w")),
            StreamHandler(),
        ],
    )

    args.func(args)


if __name__ == "__main__":
    cli()
