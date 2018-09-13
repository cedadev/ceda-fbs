"""
Script to be run as a cron job to update the FBS indexes as files are added to the archive.
This is achieved by reading the deposit logs and scanning for DEPOSIT and REMOVE tags.


Usage:
    fbs_live_index.py --help
    fbs_live_index.py --version
    fbs_live_index.py (-d DIR | --dir DIR )(-l LEVEL | --level LEVEL)(-i INDEX | --index INDEX)[--logdir LOGDIR]

Options:
    --help              Display help.
    --version           Show Version.
    -d --dir            Directory to put the lists created from deposit logs and to read from for the scanning.
    -l --level          Elasticsearch detail level
    -i --index          Index to modify
    --logdir             Logging directory





"""
from docopt import docopt

from ceda_elasticsearch_tools.core.log_reader import DepositLog
from ceda_elasticsearch_tools.cmdline import __version__
from ceda_elasticsearch_tools.core.utils import get_latest_log

import os
import subprocess
import logging
import hashlib
import json
from elasticsearch import Elasticsearch

def setup_logging(config):
    if not config['LOGDIR']:
        config['LOGDIR'] = 'deposit_cron_log'

    if not os.path.isdir(config["LOGDIR"]):
        os.makedirs(config["LOGDIR"])

    logger = logging.getLogger(__name__)
    handler = logging.FileHandler(os.path.join(config['LOGDIR'], 'elasticsearch_deposit_cron.log'))
    formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    return logger


def delete_json(file_list, config):
    bulk_delete_json = ""

    for file in file_list:
        file_id = hashlib.sha1(file).hexdigest()
        bulk_delete_json += json.dumps({"delete": {"_index": config["INDEX"], "_type": "file", "_id": file_id}}) + '\n'

    return bulk_delete_json



def main():
    config = docopt(__doc__, version=__version__)

    logger = setup_logging(config)

    # Check if output directory exists.
    if not os.path.isdir(config['DIR']):
        os.makedirs(config['DIR'])

    # Get all relevant deposit logs
    deposit_logs = get_latest_log("/badc/ARCHIVE_INFO/deposit_logs", "deposit_ingest", rank=-2)

    for log in deposit_logs:
        print (log)
        dl = DepositLog(log_filename=log)

        #################################################
        #                                               #
        #            Process file depositions           #
        #                                               #
        #################################################

        deposit_output_file = os.path.splitext(dl.filename)[0] + "_DEPOSIT.txt"

        # Check if file has already been scanned
        if deposit_output_file in os.listdir(config["DIR"]):
            # File has already been scanned
            logger.info("Deposit log {} has already been processed.".format(log))

        else:
            python_executable = "/home/badc/software/fbs/venv-ceda-fbs/bin/python"
            python_script_path="{} ceda-fbs/python/src/fbs/cmdline/scan_dataset.py".format(python_executable)

            if dl.deposit_list:
                file_list_path = os.path.join(config['DIR'], deposit_output_file)
                dl.write_filelist(file_list_path)


                command = "{python_script} -f {dataset} -n {num_files} -s 0 -l {level} -i {index}".format(
                    python_script=python_script_path,
                    dataset=file_list_path,
                    num_files=len(dl.deposit_list),
                    level=config["LEVEL"],
                    index=config["INDEX"]
                )

                print (command)

                logger.debug("Running command: {}".format(command))
                try:
                    subprocess.call(command, shell=True)
                except Exception:
                    logger.error("Elasticsearch update failed")
                    raise

                logger.info("{} files indexed".format(len(dl.deposit_list)))


        #################################################
        #                                               #
        #             Process file deletions            #
        #                                               #
        #################################################

        delete_output_file = os.path.splitext(dl.filename)[0] + "_REMOVE.txt"

        if delete_output_file in os.listdir(config['DIR']):
            # File has been scanned, log messages and exit
            logger.info("Deposit log has already been processed")

        else:
            if dl.deletion_list:
                success = 0
                fail = 0

                # Create json to request deletion
                delete_request = delete_json(dl.deletion_list, config)
                es = Elasticsearch([{"host":"jasmin-es1.ceda.ac.uk","port":9200}])
                r = es.bulk(index=config["INDEX"], body=delete_request)

                # if there were no errors log and exit
                if r["errors"] == "false":
                    logger.info("{} files successfully deleted from index: {}".format(len(dl.deletion_list),config["INDEX"]))

                # Log errors
                else:
                    for item in r["items"]:
                        success += item["delete"]["_shards"]["successful"]
                        fail += item["delete"]["_shards"]["failed"]
                        logger.error("Deletion failed. Id: {}".format(item["delete"]["_id"]))

                    logger.info("Successfully deleted: {} Deletion failed: {}".format(success,fail))

if __name__== "__main__":
    main()