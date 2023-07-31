import logging
import subprocess
import sys
import shutil
from datetime import datetime
from logging import FileHandler
from logging import StreamHandler
from psutil import virtual_memory
from pathlib import Path
from argparse import ArgumentParser
from tempfile import TemporaryDirectory
from spatial_inventory_rollback.gcbm.merge import gcbm_merge
from spatial_inventory_rollback.gcbm.merge import gcbm_merge_tile
from spatial_inventory_rollback.gcbm.merge.gcbm_merge_input_db import replace_direct_attached_transition_rules
from gcbmwalltowall.builder.projectbuilder import ProjectBuilder
from gcbmwalltowall.configuration.configuration import Configuration
from gcbmwalltowall.configuration.gcbmconfigurer import GCBMConfigurer
from gcbmwalltowall.component.project import Project
from gcbmwalltowall.component.preparedproject import PreparedProject

def build(args):
    logging.info(f"Building {args.config_path}")
    ProjectBuilder.build_from_file(args.config_path, args.output_path)

def prepare(args):
    config = Configuration.load(args.config_path, args.output_path)
    project = Project.from_configuration(config)
    logging.info(f"Preparing {project.name}")

    project.tile()
    project.create_input_database(config.recliner2gcbm_exe)
    project.run_rollback(config.recliner2gcbm_exe)

    extra_args = {
        param: config.get(param) for param in ("start_year", "end_year")
        if config.get(param)
    }

    project.configure_gcbm(config.gcbm_template_path,
                           config.gcbm_disturbance_order,
                           **extra_args)

def merge(args):
    with TemporaryDirectory() as tmp:
        projects = [PreparedProject(path) for path in args.project_paths]
        logging.info("Merging projects:\n{}".format("\n".join((str(p.path) for p in projects))))
        inventories = [project.prepare_merge(tmp, i) for i, project in enumerate(projects)]

        output_path = Path(args.output_path)
        merged_output_path = output_path.joinpath("layers", "merged")
        tiled_output_path = output_path.joinpath("layers", "tiled")
        db_output_path = output_path.joinpath("input_database")

        shutil.rmtree(merged_output_path, ignore_errors=True)
        
        start_year = min((project.start_year for project in projects))
        end_year = max((project.end_year for project in projects))

        memory_limit = virtual_memory().available * 0.75 // 1024**2
        merged_data = gcbm_merge.merge(
            inventories, str(merged_output_path), str(db_output_path),
            start_year, memory_limit_MB=memory_limit)

        gcbm_merge_tile.tile(
            str(tiled_output_path), merged_data, inventories,
            args.include_index_layer)

        replace_direct_attached_transition_rules(
            str(db_output_path.joinpath("gcbm_input.db")),
            str(tiled_output_path.joinpath("transition_rules.csv")))

        config = Configuration.load(args.config_path, args.output_path)
        configurer = GCBMConfigurer(
            [str(tiled_output_path)], config.gcbm_template_path,
            str(db_output_path.joinpath("gcbm_input.db")),
            str(output_path.joinpath("gcbm_project")), start_year, end_year,
            config.gcbm_disturbance_order)
    
        configurer.configure()

def run(args):
    project = PreparedProject(args.project_path)
    logging.info(f"Running project ({args.host}):\n{project.path}")

    with project.temporary_new_end_year(args.end_year):
        config = (
            Configuration.load(args.config_path, args.project_path)
            if args.config_path
            else Configuration({}, "")
        )

        if args.host == "local":
            logging.info(f"Using {config.resolve(config.gcbm_exe)}")
            subprocess.run([
                str(config.resolve(config.gcbm_exe)),
                "--config_file", "gcbm_config.cfg",
                "--config_provider", "provider_config.json"
            ], cwd=project.gcbm_config_path)
        elif args.host == "cluster":
            logging.info(f"Using {config.resolve(config.distributed_client)}")
            project_name = config.get("project_name", project.path.stem)
            subprocess.run([
                sys.executable, str(config.resolve(config.distributed_client)),
                "--title", datetime.now().strftime(f"gcbm_{getattr(args, 'title', project_name)}_%Y%m%d_%H%M%S"),
                "--gcbm-config", str(project.gcbm_config_path.joinpath("gcbm_config.cfg")),
                "--provider-config", str(project.gcbm_config_path.joinpath("provider_config.json")),
                "--study-area", str(
                    (project.rollback_layer_path or project.tiled_layer_path)
                    .joinpath("study_area.json")),
                "--no-wait"
            ], cwd=project.path)

def cli():
    parser = ArgumentParser(description="Manage GCBM wall-to-wall projects")
    parser.set_defaults(func=lambda _: parser.print_help())
    subparsers = parser.add_subparsers(help="Command to run")
    
    build_parser = subparsers.add_parser(
        "build",
        help=("Use the builder configuration contained in the config file to fill in and "
              "configure the rest of the project; overwrites the existing json config file "
              "unless output config file path is specified."))
    build_parser.set_defaults(func=build)
    build_parser.add_argument(
        "config_path",
        help="path to config file containing shortcut 'builder' section")
    build_parser.add_argument(
        "output_path", nargs="?", help="destination directory for build output")

    prepare_parser = subparsers.add_parser(
        "prepare",
        help=("Using the project configuration in the config file, tile the spatial "
              "layers, generate the input database, run the spatial rollback if "
              "specified, and configure the GCBM run."))
    prepare_parser.set_defaults(func=prepare)
    prepare_parser.add_argument(
        "config_path",
        help="path to config file containing fully-specified project configuration")
    prepare_parser.add_argument(
        "output_path", nargs="?", help="destination directory for project files")

    merge_parser = subparsers.add_parser(
        "merge",
        help="Merge two or more walltowall-prepared inventories together.")
    merge_parser.set_defaults(func=merge, include_index_layer=False)
    merge_parser.add_argument(
        "config_path",
        help="path to walltowall config file for disturbance order and GCBM config templates")
    merge_parser.add_argument(
        "project_paths", nargs="+",
        help="root directories of at least two walltowall-prepared projects")
    merge_parser.add_argument(
        "--output_path", required=True,
        help="path to generate merged output in")
    merge_parser.add_argument(
        "--include_index_layer", action="store_true",
        help="include merged index as reporting classifier")

    run_parser = subparsers.add_parser(
        "run", help="Run the specified project either locally or on the cluster.")
    run_parser.set_defaults(func=run)
    run_parser.add_argument(
        "host", choices=["local", "cluster"], help="run either locally or on the cluster")
    run_parser.add_argument(
        "project_path", help="root directory of the walltowall-prepared project to run")
    run_parser.add_argument(
        "--config_path",
        help="path to config file containing fully-specified project configuration")
    run_parser.add_argument(
        "--end_year", type=int, help="temporarily set a new end year for this run")
    run_parser.add_argument(
        "--title", help="explicitly specify a title for this run")

    args = parser.parse_args()

    log_path = Path(
        args.output_path if getattr(args, "output_path", None)
        else args.project_path if getattr(args, "project_path", None)
        else "."
    ).joinpath("walltowall.log")

    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", handlers=[
        FileHandler(log_path, mode=("a" if args.func == run else "w")),
        StreamHandler()
    ])

    args.func(args)

if __name__ == "__main__":
    cli()
