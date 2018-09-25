#!/usr/bin/env python

"""
Script to be run as a cron job to update the FBS indexes as files are added to the archive.
This is achieved by reading the deposit logs and scanning for DEPOSIT and REMOVE tags.


Usage:
    fbs_live_index.py --help
    fbs_live_index.py --version
    fbs_live_index.py (-d DIR | --dir DIR )(-l LEVEL | --level LEVEL)(-i INDEX | --index INDEX)(-s STREAM | --stream STREAM)

Options:
    --help              Display help.
    --version           Show Version.
    -d --dir            Directory to put the file lists for scanning.
    -l --level          FBS detail level (1|2|3)
    -i --index          Index to modify
    -s --stream         Deposit stream to follow






"""

from docopt import docopt

from ceda_elasticsearch_tools.core.log_reader import DepositLog
from ceda_elasticsearch_tools.core.utils import get_latest_log
from ceda_elasticsearch_tools.index_tools.index_updaters import CedaFbi
import os
import subprocess
import hashlib

class FbsLiveIndex():

    def __init__(self, args):
        self._process_args(args)
        self.log_path = "/badc/ARCHIVE_INFO/deposit_logs"

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

        # Index update object
        self.ceda_fbi_updater = CedaFbi(index=self.INDEX, host="jasmin-es1.ceda.ac.uk", port=9200)

    def _process_args(self, args):
        for arg, value in args.iteritems():
            if not arg.startswith("-"):
                setattr(self, arg, value)
        self.args = args

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

        if log == self.previous_logfile:
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
        """"""
        if not file_list:
            return

        # Write list of files to disk
        file_to_scan = os.path.join(self.DIR, "deposit_list_{}.txt".format(self.STREAM))

        self._write_filelist(file_to_scan, file_list)

        # Create command
        # python_executable = "/home/badc/software/fbs/venv-ceda-fbs/bin/python"
        python_executable = "/group_workspaces/jasmin4/cedaproc/rsmith013/fbs-dev/venv-ceda-fbs/bin/python"
        python_script_path = "{} ceda-fbs/python/src/fbs/cmdline/scan_dataset.py".format(python_executable)

        command = "{python_script} -f {dataset} -n {num_files} -s {start} -l {level} -i {index}".format(
            python_script=python_script_path,
            dataset=file_to_scan,
            num_files=len(file_list),
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

        deletion_list = []

        # Prepare data for updater class
        for file in file_list[start_index:]:
            deletion_list.append({
                "id": hashlib.sha1(file).hexdigest()
            })

        # Process the list
        return_state = self.ceda_fbi_updater.delete_files(deletion_list)

        if return_state["failed"] == 0:
            self.delete_status = start_index

        else:
            self.delete_status = start_index
            raise Exception("Delete incomplete. Failed: {} Successful: {}".format(return_state["failed"], return_state["success"]))

    def _process_log(self, log_dict):
        """

        :param filename:
        :return:
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

        :return:
        """

        # The log in the state file is the latest and the previous log has been processed
        if self.previous_logfile["deposit"] == -1:
            self._process_log(self.current_logfile)

        # The log in the state file is the previous log so need to process both the latest and previous
        else:
            self._process_log(self.previous_logfile)
            self._process_log(self.current_logfile)

    def process_logs(self):

        self._read_status_file()

        self._check_deposits()

        self._update_status_file()


if __name__ == "__main__":
    config = docopt(__doc__, version="1.0")

    live_index = FbsLiveIndex(config).process_logs()
