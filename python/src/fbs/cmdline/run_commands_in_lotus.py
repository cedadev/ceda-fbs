"""
Usage:
  run_commands_in_lotus.py --help
  run_commands_in_lotus.py --version
  run_commands_in_lotus.py (-f <filename> | --filename <filename>)
                           [-p <number_of_processes> | --num-processes <number_of_processes>]

Options:
  --help                                     Show this screen.
  --version                                  Show version.
  -f --filename=<filename>                   File from where the dataset
                                             will be read
                                             [default: datasets.ini].
  -p --num-processes=<number_of_processes>   Number of processes to use.
"""

import os
from docopt import docopt
import src.fbs.proc.common_util.util as util
from cmdline import __version__  # Grab version from package __init__.py
import datetime


def main():

    start = datetime.datetime.now()
    print "==============================="
    print "Script started at: %s." % start

    # Gets command line arguments.
    com_args = util.sanitise_args(docopt(__doc__, version=__version__))
    commands_file = com_args["filename"]
    num_processes = com_args["num-processes"]

    util.run_tasks_file_in_lotus(commands_file, int(num_processes), user_wait_time=None, logger=None)

    end = datetime.datetime.now()
    print "Script ended at: %s, it ran for: %s." % (end, (end - start))
    print "==============================="


if __name__ == '__main__':

    main()
