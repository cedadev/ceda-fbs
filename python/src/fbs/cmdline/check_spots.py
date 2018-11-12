#!/usr/bin/env python

"""
Generate a list of files which are missing from the fbs index based on an input list/ground truth


Usage:
    check_spots.py --help
    check_spots.py --version
    check_spots.py SPOT_NAME SPOT_PATH [--deleted]

Options:
    --help              Display help.
    --version           Show Version.
    --deleted           Tells the script to find the deleted files by comparing files which are in the index but not on the file system

"""

from docopt import docopt

from ceda_elasticsearch_tools.index_tools.index_updaters import CedaFbi
from ceda_elasticsearch_tools.core.utils import list2file_newlines
import proc.common_util.util as util
import os
from collections import OrderedDict
import hashlib


class IncompatibleIndexError(Exception):
    pass


class InaccurateSpot():
    missing_files = []
    files_to_test = []
    input_file_list = []

    def __init__(self, spot_name, spot_path, index='ceda-fbi', host='jasmin-es1.ceda.ac.uk', port=9200):
        """
        Open file and read input file list


        :param spot_file:   File list of all files in the spot
        """

        self.spot_name = spot_name

        print ("Generating file list")
        self.input_file_list = util.build_file_list(spot_path)

        print ("Generated file list. Files: {:,}".format(len(self.input_file_list)))

        if index == 'ceda-fbi':
            self.ES = CedaFbi(index, host, port)

        else:
            raise IncompatibleIndexError

    def create_directories(self):
        """
        Take the input file list and split files into direcctory structure
        :return: OrderedDictionary of directories with their associate files
        """

        dirs_dict = OrderedDict()

        # Setup empty list for each directory
        for file in self.input_file_list:
            dirs_dict[os.path.dirname(file)] = []

        for file in self.input_file_list:
            dirs_dict[os.path.dirname(file)].append(file)

        print ("Directories: {:,}".format(len(dirs_dict)))

        return dirs_dict

    def run_directory_check(self):
        """
        Take the file list ordered into directories and compare the number for files in each directory
        with the number of files under that directory in the elasticsearch index.

        This is a rough check to try and reduce the number of calls to elasticsearch (ES) and focus on the
        directories where the cound in ES is lower.

        Process the response from ES and only append file lists where there were fewer files in ES than
        the ground truth list.


        :param index:
        :return:
        """

        ordered_dirs_dict = self.create_directories()

        elasticsearch_response = self.ES.check_dir_count(ordered_dirs_dict)

        dict_keys = ordered_dirs_dict.keys()

        for i, block in enumerate(elasticsearch_response):
            base = 800 * i
            for j, response in enumerate(block["responses"]):
                relevant_dir = ordered_dirs_dict[dict_keys[base + j]]
                matches = response["aggregations"]["file_count"]["value"]

                if matches == 0 and len(relevant_dir) > 0:
                    self.missing_files.extend(relevant_dir)

                elif matches != len(relevant_dir):
                    self.files_to_test.extend(relevant_dir)

        print ("Test List: {:,} Missing files: {:,}".format(len(self.files_to_test), len(self.missing_files)))

    def run_existence_check(self, test_list=None, index="ceda-fbi"):
        """
        Take output from run_directory_check and test the reduced file lists to see which file are
        actually missing from the index.

        """

        if test_list is None:
            test_list = self.files_to_test

        elasticsearch_response = self.ES.check_files_existence(test_list)

        for i, block in enumerate(elasticsearch_response):
            base = 800 * i

            for j, response in enumerate(block):
                matches = len(response)

                if matches == 1:  # Unique match
                    continue

                elif matches == 0:
                    self.missing_files.append(test_list[base + j])

                else:
                    raise ValueError(
                        "Query has responded with more than one match. Was expecting 1 or 0. Response: {}".format(
                            response))

    def get_all_records_in_spot(self):

        query = {
            "query": {
                "term": {
                    "info.spot_name.keyword": {
                        "value": self.spot_name
                    }
                }
            }
        }

        results = self.ES._scroll_search(query,size=1000)

        return [os.path.join(x['_source']['info']['directory'], x["_source"]['info']['name']) for x in results]

    def find_missing_files(self, output_dir):
        """

        Finds the files which are on disk but not in the Elasticsearch index.
        1. Compare directories to discount any directories which are complete
        2. Any incomplete directories, the files are checked in ES 1 by 1
        3. Compile list of missing files
        4. Write this list to disk


        :param output_dir: Output directory to put file list
        :return:

        Writes a files called <spot_name>_missing.txt
        """

        self.run_directory_check()
        self.run_existence_check()

        if self.missing_files:
            print ("Total Missing: {:,} Spot: {}".format(len(self.missing_files), self.spot_name))

            list2file_newlines(self.missing_files,
                               os.path.join(output_dir, "{}_missing.txt".format(self.spot_name)))

    def find_deleted_files(self):
        """

        1. Check the es <-> file counts
        2. Page download all es entries
        3. Set subtraction to get the difference
        4. Write deleted files to disk

        :return

        Writes a files called <spot_name>_deleted.txt
        """

        query = {
            "query": {
                "term": {
                    "info.spot_name.keyword": {
                        "value": self.spot_name
                    }
                }
            }
        }

        response = self.ES.es.search(index='ceda-fbi', body=query)

        if response["hits"]["total"] > len(self.input_file_list):

            all_records = self.get_all_records_in_spot()

            deleted_files = set(all_records) - set(self.input_file_list)

            content_to_delete = []
            for file in deleted_files:
                content_to_delete.append({
                    'id': hashlib.sha1(file).hexdigest()
                })

            self.ES.delete_files(content_to_delete)


if __name__ == "__main__":
    config = docopt(__doc__, version="1.0")

    SPOT_NAME = config["SPOT_NAME"]
    SPOT_PATH = config["SPOT_PATH"]
    OUTPUT_DIR = config["OUTPUT"]

    deleted = config['--deleted']

    inaccurate_spot = InaccurateSpot(SPOT_NAME, SPOT_PATH)

    if not deleted:
        inaccurate_spot.find_missing_files(output_dir=OUTPUT_DIR)

    else:

        inaccurate_spot.find_deleted_files()
