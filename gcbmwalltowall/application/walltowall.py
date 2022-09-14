import logging
import sys
from argparse import ArgumentParser
from gcbmwalltowall.configuration.configuration import Configuration
from gcbmwalltowall.component.project import Project

if __name__ == "__main__":
    cli()

def cli():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

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
        "--output_path",
        help="path to output config file to generate, otherwise overwrite existing")

    prepare_parser = subparsers.add_parser(
        "prepare",
        help=("Using the project configuration in the config file, tile the spatial "
              "layers, generate the input database, run the spatial rollback if "
              "specified, and configure the GCBM run."))
    prepare_parser.set_defaults(func=prepare)
    prepare_parser.add_argument(
        "config_path",
        help="path to config file containing fully-specified project configuration")

    run_parser = subparsers.add_parser(
        "run", help="Run the specified project either locally or on the cluster.")
    run_parser.add_argument(
        "host", choices=["local", "cluster"], help="run either locally or on the cluster")
    run_parser.set_defaults(func=run)

    args = parser.parse_args()
    args.func(args)

def build(args):
    print(args)

def prepare(args):
    config = Configuration.load(args.config_path)
    project = Project.from_configuration(config)
    project.tile()

def run(args):
    print(args)
