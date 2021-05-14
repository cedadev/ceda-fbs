#!/usr/bin/env python

"""
Usage:
  make_file_lists.py -h | --help
  make_file_lists.py --version
  make_file_lists.py (-f <filename> | --filename <filename>)
                     (-m <location> | --make-list <location>)
                     (--host <hostname>)
                     [--followlinks]

Options:
  -h --help                                  Show this screen.

  --version                                  Show version.

  -f --filename=<filename>                   File from where the dataset
                                             will be read
                                             [default: datasets.ini].

  -m --make-list=<location>                  Stores the list of filenames
                                             to a file.

  --host=<hostname>                          The name of the host where
                                             the script will run.

  --followlinks                              Follow symlinks in the os walk
 """

import os

from docopt import docopt
import fbs.proc.common_util.util as util
from cmdline import __version__  # Grab version from package __init__.py
import datetime
import subprocess
import fbs.proc.constants.constants as constants

SCRIPT_DIR = os.path.realpath(os.path.dirname(__file__))


def get_stat_and_defs(com_args):
    """
    Sets variables that determine the operations to be performed.
    """

    status_and_defaults = []
    # Searches for the configuration file.
    if 'config' not in com_args or not com_args["config"]:
        direc = os.path.dirname(__file__)
        conf_path = os.path.join(direc, "../../../config/ceda_fbs.ini")
        com_args["config"] = conf_path

    # Creates a dictionary with default settings some of
    # them where loaded from the defaults file.
    config = util.get_settings(com_args["config"], com_args)

    status_and_defaults.append(config)

    if ("host" in config) and config["host"] == "localhost":
        status_and_defaults.append(constants.Script_status.RUN_SCRIPT_IN_LOCALHOST)
    else:
        status_and_defaults.append(constants.Script_status.RUN_SCRIPT_IN_LOTUS)

    return status_and_defaults


def store_datasets_to_files(status, config, host):
    """
    Finds and stores all files belonging to each dataset.
    """

    # Get file.
    filename = config["filename"]

    # Extract datasets ids and paths.
    datasets = util.find_dataset(filename, "all")
    scan_commands = []
    directory_to_save_files = config["make-list"]

    # Create the commands that will create the
    # files containing the paths to data files.
    for dataset in datasets:

        command = f"{SCRIPT_DIR}/scan_dataset.py -f {filename} -d  {dataset} --make-list {os.path.join(directory_to_save_files, dataset)}.txt"

        # Add followlinks flag if follow links flag is used
        if config['followlinks']:
            command += ' --followlinks'

        # If using localhost, execute script immediately
        if host == 'localhost':
            print(f"Executing: {command}")
            subprocess.call(command, shell=True)

        # Otherwise append to list
        else:
            scan_commands.append(command)

    # Execute commands on lotus
    if host == 'lotus':
        lotus_runner = util.LotusRunner(queue='short-serial')
        lotus_runner.run_tasks_in_lotus(scan_commands)


def main():
    """
    Relevant ticket : http://team.ceda.ac.uk/trac/ceda/ticket/23217
    """

    # Get command line arguments.
    com_args = util.sanitise_args(docopt(__doc__, version=__version__))

    # Insert defaults
    status_and_defaults = get_stat_and_defs(com_args)

    start = datetime.datetime.now()
    print(f"Script started at: {start}")

    status = status_and_defaults[1]
    config = status_and_defaults[0]

    if status == constants.Script_status.RUN_SCRIPT_IN_LOCALHOST:
        store_datasets_to_files(status, config, 'localhost')
    else:
        store_datasets_to_files(status, config, 'lotus')

    end = datetime.datetime.now()
    print(f"Script ended at : {end} it ran for : {end-start}")


if __name__ == '__main__':
    main()
