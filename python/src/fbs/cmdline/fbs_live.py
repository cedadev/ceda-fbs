#!/usr/bin/env python

"""
Script to be run as a cron job to update the FBS indexes as files are added to the archive.
This is achieved by reading the deposit logs and scanning for DEPOSIT and REMOVE tags.


Usage:
    fbs_live_index.py --help
    fbs_live_index.py --version
    fbs_live_index.py (-s STREAM | --stream STREAM) (--config CONFIG)

Options:
    --help              Display help.
    --version           Show Version.
    -s --stream         Deposit stream to follow
    --config            File containing configuration options


"""

from docopt import docopt

from ceda_elasticsearch_tools.core.log_reader import DepositLog
from ceda_elasticsearch_tools.core.utils import get_latest_log
from ceda_elasticsearch_tools.index_tools.index_updaters import CedaFbi
import os
import subprocess
import hashlib
import fbs.proc.common_util.util as util
import six


class FbsLiveIndex():

    def __init__(self, args):
        self._process_args(args)

        # Log status
        self.previous_logfile = {
            "log": get_latest_log(self.log_path, self.STREAM, rank=-2)[0],
            "delete": 0,
            "deposit": 0
        }

        self.current_logfile = {
            "log": get_latest_log(self.log_path, self.STREAM, rank=-1)[0],
            "delete": 0,
            "deposit": 0
        }

        # Set status file
        self.status_file_name = os.path.join(self.DIR, self.STREAM + "_status.txt")
        self.deposit_status = 0
        self.delete_status = 0

        kwargs = {
            "http_auth": (
                self.conf["es-configuration"]["es-username"],
                self.conf["es-configuration"]["es-password"]
            )
        }

        # Index update object
        self.ceda_fbi_updater = CedaFbi(index=self.INDEX, host_url=self.conf["es-configuration"]["es-url"], **kwargs)

    def _process_args(self, args):
        """
        Turn the docopt arguments into object properties
        and read config file
        :param args: Docopt arguments
        """

        for arg, value in six.iteritems(args):
            if not arg.startswith("-"):
                setattr(self, arg, value)
        self.args = args

        self.conf = util.cfg_read(self.CONFIG)

        self.log_path = self.conf["core"]["source-log-directory"]
        self.DIR = self.conf["core"]["status-file-directory"]
        self.INDEX = self.conf["es-configuration"]["es-index"]
        self.LEVEL = self.conf["core"]["scan-level"]


    def _read_status_file(self):
        """
        Check the status file for the given stream to find out how far along the process the file has got.

        """

        # Read progress file
        try:
            with open(self.status_file_name) as reader:
                status_line = reader.readline().strip()
                deposit, delete, log = status_line.split()

        except (IOError, ValueError):
            # Status file not present. Set properties to scan from most recent file.
            self.previous_logfile["deposit"] = -1
            self.previous_logfile["delete"] = -1
            return

        if log == self.previous_logfile["log"]:

            # The penultimate log is listed in the state file which means that it is possible
            # this file has not completed scanning. Return the progress in this file and
            # set the progress to the latest log at the start of the file.
            self.previous_logfile["deposit"] = int(deposit)
            self.previous_logfile["delete"] = int(delete)

        else:
            # The latest log is listed in the state file which means that the previous log has been completed.
            # Return a -1 code to indicate complete and return the current progress of the latest log.
            self.previous_logfile["deposit"] = -1
            self.previous_logfile["delete"] = -1
            self.current_logfile["deposit"] = int(deposit)
            self.current_logfile["delete"] = int(delete)

    def _update_status_file(self):
        """
        Update the status file with the current stopping point

        """

        status_line = "{deposit} {delete} {log}".format(deposit=self.deposit_status, delete=self.delete_status,
                                                        log=self.current_logfile["log"])
        with open(self.status_file_name, "w") as writer:
            writer.write(status_line)

    def _write_filelist(self, path, list):
        """
        :param path: The path to file to write
        :param list: List of strings to write to file
        """
        with open(path, "w") as writer:
            writer.writelines(map(lambda x: x + "\n", list))

    def _process_deposits(self, start_index, file_list):
        """
        Process deposits from deposit logs

        :param start_index: File to start the scan from
        :param file_list:   List of files to process

        """

        if not file_list:
            return

        # Write list of files to disk
        file_to_scan = os.path.join(self.DIR, "deposit_list_{}.txt".format(self.STREAM))

        self._write_filelist(file_to_scan, file_list)

        # Create command
        python_executable = self.conf["core"]["python-executable"]
        python_script_path = "{} ceda-fbs/python/src/fbs/cmdline/scan_dataset.py".format(python_executable)

        command = "{python_script} -f {dataset} -n {num_files} -s {start} -l {level} -i {index}".format(
            python_script=python_script_path,
            dataset=file_to_scan,
            num_files=len(file_list)-start_index,
            start=start_index,
            level=self.LEVEL,
            index=self.INDEX
        )

        print (command)

        # Run the command
        rc = subprocess.call(command, shell=True)

        if rc == 0:
            # successful
            self.deposit_status = len(file_list)
        else:
            # unsuccessful, don't update progress
            self.deposit_status = start_index
            raise Exception(
                "Deposit incomplete. Check fbs logs for details")

    def _process_deletions(self, start_index, file_list):
        """
        Process deletions contained in the deposit logs

        :param start_index: File to start the deletions from
        :param file_list:   List of files to process
        """

        deletion_list = []

        # Prepare data for updater class
        for file in file_list[start_index:]:
            deletion_list.append({
                "id": hashlib.sha1(file).hexdigest()
            })

        # Process the list
        return_state = self.ceda_fbi_updater.delete_files(deletion_list)

        self.delete_status = len(file_list)

    def _process_log(self, log_dict):
        """
        Process the given log

        :param log_dict: Dict containing data about the log. filename, status from the status file.
        """

        print ("Filename: {}".format(log_dict["log"]))

        dl = DepositLog(log_filename=log_dict["log"])

        # Print report of actions lists
        print ("Deposit: {} Delete: {} Mkdir: {} Rmdir: {} Symlink: {} readme00: {}".format(
            len(dl.deposit_list),
            len(dl.deletion_list),
            len(dl.mkdir_list),
            len(dl.rmdir_list),
            len(dl.symlink_list),
            len(dl.readme00_list)
        ))

        # Print current state in status file
        print ("Status file - Deposit: {} Delete: {}".format(log_dict["deposit"], log_dict["delete"]))
        print ("Number of files to scan/delete: {}/{}".format(len(dl.deposit_list) - log_dict["deposit"],
                                                              len(dl.deletion_list) - log_dict["delete"])
               )

        if len(dl.deposit_list) > log_dict["deposit"]:
            self._process_deposits(log_dict["deposit"], dl.deposit_list)
        else:
            print ("Nothing to add")
            self.deposit_status = log_dict["deposit"]

        if len(dl.deletion_list) > log_dict["delete"]:
            self._process_deletions(log_dict["delete"], dl.deletion_list)
        else:
            print ("Nothing to delete")
            self.delete_status = log_dict["delete"]

    def _check_deposits(self):
        """
        Check the current working state from the status file. Decides whether need to process the previous log or just
        the most recent log.
        """

        # The log in the state file is the latest and the previous log has been processed
        if self.previous_logfile["deposit"] == -1:
            self._process_log(self.current_logfile)

        # The log in the state file is the previous log so need to process both the latest and previous
        else:
            self._process_log(self.previous_logfile)
            self._process_log(self.current_logfile)

    def process_logs(self):
        """
        Main entry point to class to run the processing chain
        """

        self._read_status_file()

        self._check_deposits()

        self._update_status_file()


if __name__ == "__main__":
    config = docopt(__doc__, version="1.0")

    live_index = FbsLiveIndex(config).process_logs()
