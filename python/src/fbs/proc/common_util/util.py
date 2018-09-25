"""
Module containing useful functions for the command-line interfaces.
"""

import os
import errno
import sys
import subprocess

import simplejson as json
import time
from enum import Enum
import ConfigParser
import logging
import re
import io
import datetime
from dateutil import parser
import hashlib

log_levels = {"debug": logging.DEBUG,
              "info": logging.INFO,
              "warning": logging.WARNING,
              "error": logging.ERROR,
              "critical": logging.CRITICAL
              }

MAX_ATTR_LENGTH = 256


class Parameter(object):
    """
    Placeholder/wrapper class for metadata parameters

    :param str name: Name of variable/parameter
    :param dict other_params: Optional - Dict containing other param metadata
    """

    def __init__(self, name, other_params=None):
        self.items = []
        self.name = name

        # Other arbitrary arguments
        if other_params:
            for key, value in other_params.iteritems():
                self.items.append(
                    self.make_param_item(key.strip(), unicode(value).strip()))

    @staticmethod
    def make_param_item(name, value):
        """
        Convert a name/value pair to dictionary (for better indexing in ES)

        :param str name: Name of the parameter item (e.g. "long_name_fr", etc)
        :param str value: Value of the parameter item (e.g. "Radiance")
        :return: Dict containing name:value information
        """
        return {"name": name,
                "value": value}

    def get(self):
        """Return the list of parameter items"""
        return self.items

    def get_name(self):
        """Return the name of the phenomenon."""
        return self.name


class FileFormatError(Exception):
    """
    Exception to raise if there is a error in the file format
    """
    pass


def delete_folder(folder):
    try:
        os.rmdir(folder)  # This deletes only empty dirs.
    except OSError as ex:
        if ex.errno == errno.ENOTEMPTY:
            pass


def sanitise_args(config):
    """
    Sanitise command-line configuration.

    :param config: Config dictionary (from docopt)
    :returns: Config dictionary with all keys stripped of '<' '>' and '--'
    """
    sane_conf = {}
    for key, value in config.iteritems():
        if value is not None:
            key = key.lstrip("-><").rstrip("><")
            sane_conf[key] = value

    return sane_conf


def read_conf(conf_path):
    """
    Reads configuration file into a dictionary.

    :param conf_path: Path to the JSON configuration file.
    :returns: Dict containing parsed JSON conf.
    """
    try:
        with open(conf_path, "r") as conf:
            return json.load(conf)
    except IOError:
        err_path = os.path.abspath(conf_path)
        sys.stderr.write(  # Continued on next line
            "Can't read configuration file\n%s\n\n" % err_path)
        return {}


def cfg_read(filename):
    """
    Reads configuration file into a dictionary.

    :param filename: Path to the INI configuration file.
    :returns: Dict containing parsed ini conf.
    """
    # Read the config file
    config = ConfigParser.ConfigParser()
    config.read(filename)

    # get sections
    sections = config.sections()

    conf = {}
    section_options = {}
    handlers_sections = []

    for section in sections:

        if section in handlers_sections:
            continue

        options = config.options(section)

        for option in options:

            try:
                value = config.get(section, option)
                parsed_value = value.replace("\"", "")
                if section == "handlers":
                    handlers_sections.append(value)
                section_options[option] = parsed_value
                if section_options[option] == -1:
                    section_options[option] = None
            except:
                section_options[option] = None

        conf[section] = section_options.copy()
        section_options.clear()

    regx_details = {}
    regxs = {}
    for handler in handlers_sections:
        regx_pattern = config.get(handler, "regx")
        regx_details["class"] = config.get(handler, "class")
        regx_details["priority"] = config.get(handler, "priority")
        regxs[regx_pattern] = regx_details.copy()
        regx_details.clear()

    conf["handlers"] = regxs.copy()

    return conf


def get_settings(conf_path, args):
    # Default configuration options
    # These are overridden by the config file and command-line arguments
    defaults = {}

    # conf_file = read_conf(conf_path)
    conf_file = cfg_read(conf_path)

    # print conf_file

    # Apply updates to CONFIG dictionary in priority order
    # Configuration priority: CONFIG < CONF_FILE < ARGS
    # (CONFIG being lowest, ARGS being highest)
    defaults.update(conf_file)
    defaults.update(args)

    return defaults


def build_file_list(path):
    """
    :param path : A file path
    :param followlinks : Bool. Sets whether os.walk should follow symbolic links.
    :return: List of files contained within the specified directory.
    """
    
    file_list = []
    for root, _, files in os.walk(path):
        for each_file in files:
            if each_file[0] == ".": continue
            file_list.append(os.path.join(root, each_file))

    return file_list


def write_list_to_file_nl(file_list, filename):
    """
    :param file_list : A list of files.
    :param filename : Where the list of files is going to be saved.
    """

    infile = open(filename, 'w')
    items_written = 0

    for item in file_list:
        infile.write("%s\n" % item)
        items_written += 1

    infile.close()
    return items_written


def write_list_to_file(file_list, filename):
    """
    :param file_list : A list of files.
    :param filename : Where the list of files is going to be saved.
    """

    infile = open(filename, 'w')
    items_written = 0

    for item in file_list:
        infile.write("%s" % item)
        items_written += 1

    infile.close()
    return items_written


def read_file_into_list(filename):
    content = []
    with open(filename) as fd:
        for line in fd:
            content.append(line)
    return content


def save_to_file(filename, data):
    with io.FileIO(filename, "a") as fp:
        fp.write(data)


def find_in_list(list_str, str_item):
    for str in list_str:
        if str_item in str:
            return str

    return None


def get_file_header(filename):
    """
    :param filename : The file to be read.
    :returns: First line of the file.
    """
    with open(filename, 'r') as fd:
        first_line = fd.readline()

    return first_line.replace("\n", "")


def get_bytes_from_file(filename, num_bytes):
    """
    :param filename : The file to be read.
    :param num_bytes : number of bytes to read.
    :returns: The first num_bytes from the file.
    """

    try:
        fd = open(filename, 'r')
        bytes_read = fd.read(num_bytes)
        fd.close()
    except IOError:
        return None

    return bytes_read


def find_dataset(filename, dataset_id):
    """
    :param filename : file containing dataset information.
    :param dataset_id : dataset to be searched.
    :returns: The path of the given dataset id.
    """
    var_dict = {}
    with open(filename) as l_file:
        for line in l_file:
            if not line.startswith("#"):
                name, var = line.partition("=")[::2]
                var_dict[name.strip()] = var.strip()

    if dataset_id == "all":
        return var_dict
    else:
        try:
            return var_dict[dataset_id]
        except KeyError as ex:
            return None


def find_num_lines_in_file(filename):
    """
    :param filename : Name of the file to be read.
    :returns: The number of lines in the given file.
    """
    num_lines = 0

    with open(filename) as infp:
        for line in infp:
            num_lines += 1
    return num_lines


def valid_attr_length(name, value):
    if len(value) < MAX_ATTR_LENGTH \
            and len(name) < MAX_ATTR_LENGTH:
        return True
    return False


def is_valid_phen_attr(attribute):
    if attribute is None:
        return True
    elif re.search(r"\d+-\d+-\d+.*", attribute) is not None:
        return False
    else:
        return True


def is_valid_parameter(name, value):
    valid_parameters = ["standard_name",
                        "long_name",
                        "title",
                        "name",
                        "units",
                        "var_id",
                        "title"
                        ]
    if name in valid_parameters \
            and valid_attr_length(name, value):
        return True
    return False


def is_valid_phenomena(key, value):
    """
    Wrapper to hide test in main function
    """

    if not is_valid_parameter(key, value):
        return False

    if not is_valid_phen_attr(value):
        return False

    # Returns true if both the tests above pass as true
    return True


def get_number_of_submitted_lotus_tasks(max_number_of_tasks_to_submit):
    """
    :returns: Number of tasks submitted in lotus.
    """

    empty_task_queue_string = "No unfinished job found\n"
    non_empty_task_queue_string = "JOBID     USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME"

    command_output = subprocess.check_output('bjobs', stderr=subprocess.STDOUT, shell=True)

    if command_output == empty_task_queue_string:
        num_of_running_tasks = 0
    elif command_output.startswith(non_empty_task_queue_string):
        num_of_running_tasks = command_output.count("\n") - 1
    else:
        num_of_running_tasks = max_number_of_tasks_to_submit

    return num_of_running_tasks


def is_date_valid(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def run_tasks_in_lotus(task_list, max_number_of_tasks_to_submit, user_wait_time=None, queue='par-single', logger=None):
    """
    :param task_list : list of commands to run.
    :param max_number_of_tasks_to_submit : max number of jobs to be run in parallel.
    :param user_wait_time : polling time.
    :param logger : object used for logging.

    Submits the commands supplied in lotus making sure that
    max_number_of_jobs is not exceeded.
    """

    if user_wait_time is None:
        init_wait_time = 30
    else:
        init_wait_time = user_wait_time

    wait_time = init_wait_time
    dec = 1
    iterations_counter = 0

    info_msg = "Max number of jobs to submit in each step : %s.\
               \nTotal number commands to run : %s." \
               % (str(max_number_of_tasks_to_submit), str(len(task_list)))

    if logger is not None:
        logger.INFO(info_msg)
        logger.INFO("===============================")

    print info_msg
    print "==============================="

    while len(task_list) > 0:

        # Find out if other jobs can be submitted.
        try:
            num_of_running_tasks = get_number_of_submitted_lotus_tasks(max_number_of_tasks_to_submit)
        except  subprocess.CalledProcessError:
            continue

        # num_of_running_tasks = 0
        num_of_tasks_to_submit = max_number_of_tasks_to_submit - num_of_running_tasks
        iterations_counter = iterations_counter + 1

        info_msg = "Iteration : %s." % (str(iterations_counter))
        if logger is not None:
            logger.INFO(info_msg)

        print info_msg

        info_msg = "Number of jobs running  : %s." % (str(num_of_running_tasks))
        if logger is not None:
            logger.INFO(info_msg)

        print info_msg

        info_msg = "Number of jobs to submit in this step : %s." % (str(num_of_tasks_to_submit))
        if logger is not None:
            logger.INFO(info_msg)

        print info_msg

        # Submit jobs according to availability.
        for i in range(0, num_of_tasks_to_submit):

            if len(task_list) == 0:
                break

            # Is there an extract op ?
            task = task_list[0]
            task_list.remove(task)

            command = _make_bsub_command(task, i, queue=queue, logger=logger)
            subprocess.call(command, shell=True)

        info_msg = "Number of tasks waiting to be submitted : %s." % len(task_list)
        if logger is not None:
            logger.INFO(info_msg)

        print info_msg

        # Wait in case some process terminates.
        info_msg = "Waiting for : %s secs." % (str(wait_time))
        if logger is not None:
            logger.INFO(info_msg)

        print info_msg
        time.sleep(wait_time)

        # If nothing can be submitted wait again.
        if num_of_tasks_to_submit == 0:
            wait_time = wait_time - dec
            if wait_time == 0:
                wait_time = init_wait_time

        if logger is not None:
            logger.INFO("===============================")

        print "==============================="


def run_tasks_file_in_lotus(task_file, max_number_of_tasks_to_submit, user_wait_time=None, queue='par-single', logger=None):
    """
    :param task_file : file of commands to run.
    :param max_number_of_tasks_to_submit : max number of jobs to be run in parallel.
    :param user_wait_time : polling time.
    :param logger : object used for logging.

    Submits the commands supplied in lotus making sure that
    max_number_of_jobs is not exceeded.
    """

    if user_wait_time is None:
        init_wait_time = 30
    else:
        init_wait_time = user_wait_time

    wait_time = init_wait_time
    dec = 1
    iterations_counter = 0

    task_list = read_file_into_list(task_file)

    info_msg = "Max number of jobs to submit in each step : %s.\
               \nTotal number commands to run : %s." \
               % (str(max_number_of_tasks_to_submit), str(len(task_list)))

    if logger is not None:
        logger.INFO(info_msg)
        logger.INFO("===============================")

    print info_msg
    print "==============================="

    while len(task_list) > 0:

        # Find out if other jobs can be submitted.
        try:
            # num_of_running_tasks = 1
            num_of_running_tasks = get_number_of_submitted_lotus_tasks(max_number_of_tasks_to_submit)
        except  subprocess.CalledProcessError:
            continue

        # num_of_running_tasks = 0
        num_of_tasks_to_submit = max_number_of_tasks_to_submit - num_of_running_tasks
        iterations_counter = iterations_counter + 1

        info_msg = "Iteration : %s." % (str(iterations_counter))
        if logger is not None:
            logger.INFO(info_msg)

        print info_msg

        info_msg = "Number of jobs running: %s." % (str(num_of_running_tasks))
        if logger is not None:
            logger.INFO(info_msg)

        print info_msg

        info_msg = "Number of jobs to submit in this step: %s." % (str(num_of_tasks_to_submit))
        if logger is not None:
            logger.INFO(info_msg)

        print info_msg

        # Submit jobs according to availability.
        for i in range(0, num_of_tasks_to_submit):

            if len(task_list) == 0:
                break

            # Is there an extract op ?
            task = task_list[0]
            task_list.remove(task)

            command = _make_bsub_command(task, i, queue=queue, logger=logger)
            subprocess.call(command, shell=True)

            # save list to file.
            write_list_to_file(task_list, task_file)

        info_msg = "Number of tasks waiting to be submitted : %s." % len(task_list)
        if logger is not None:
            logger.INFO(info_msg)

        print info_msg

        # Wait in case some process terminates.
        info_msg = "Waiting for : %s secs." % (str(wait_time))
        if logger is not None:
            logger.INFO(info_msg)

        print info_msg
        time.sleep(wait_time)

        # If nothing can be submitted wait again.
        if num_of_tasks_to_submit == 0:
            wait_time = wait_time - dec
            if wait_time == 0:
                wait_time = init_wait_time

        if logger is not None:
            logger.INFO("===============================")

        print "==============================="


def _make_bsub_command(task, count, queue, logger=None):

    if queue == 'short-serial':
        wall_time = '24:00'
    else:
        wall_time = '48:00'

    "Construct bsub command for task and return it."
    command = "bsub -q {queue} -W {wall_time} {command}".format(queue=queue, wall_time=wall_time, command=task)
    info_msg = "%s. Executing : %s" % ((count + 1), command)
    if logger is not None: logger.INFO(info_msg)
    print info_msg
    return command

def get_best_name(phenomena):
    """
    Create a best_name field which takes the best name as defined by the preference order
    :param phenomena: phenomena attributes in form [{"name":"standard_name","value":"time"},{"name":"---","value":"---"},{}...]
    :return: best_name(string)
    """
    preference_order = ["long_name","standard_name","title","name","short_name","var_id"]
    attributes = phenomena["attributes"]

    for name in preference_order:
        best_name = [d['value'] for d in attributes if d['name'] == name]
        if best_name:
            return best_name[0]
    return None


def long_name_is_standard_name(phenomena):
    """
    If the long name and the standard name are the same and the long_name contains _
    return True
    :param phenomena: phenomena attributes in form [{"name":"standard_name","value":"time"},{"name":"---","value":"---"},{}...]
    :return: Boolean
    """
    attributes = phenomena["attributes"]

    long_name = None
    standard_name = None

    for d in attributes:
        if d["name"] == "long_name":
            long_name = d["value"]

        if d["name"] == "standard_name":
            standard_name = d["value"]

    if long_name is None or standard_name is None:
        # If one of them is missing then don't need to do next checks.
        return False

    if long_name.strip() == standard_name.strip() and '_' in long_name:
            return True
    else:
        return False


def build_phenomena(data):
    if not data:
        return (None,)

    phenom_list = []

    name_filter = ["units", "var_id", "standard_name", "long_name"]
    names_list_filter = ["standard_name", "long_name", "title", "name"]

    for phenom in data:
        phen_dict = {}
        names = []
        agg_string = ""
        agg_string_list = []

        best_name = get_best_name(phenom)

        long_name_check = long_name_is_standard_name(phenom)

        for attr in phenom["attributes"]:

            value = attr["value"]
            name = attr["name"]

            # Remove extra spaces and any quotation marks which will interfere with creating the agg_string
            value = re.sub('  +', ' ', value).replace('"', '')

            if name in name_filter:
                phen_dict[name] = value
                agg_string_list.append('"{}":"{}"'.format(name, value))

            if name in names_list_filter and value not in names:

                # if long_name containes "_" and is the same as standard name, include a version in the names
                # list with the "_" replaced by " "
                if name == "long_name" and long_name_check:
                    value = value.replace("_"," ")

                names.append('"{}"'.format(value))

        if names:
            names.sort()
            phen_dict["names"] = names
            agg_string_list.append('"names":{}'.format(';'.join(names)))

        if agg_string_list:
            agg_string_list.sort()
            agg_string = ','.join(agg_string_list)

        if best_name:
            phen_dict["best_name"] = best_name

        if phen_dict:
            phen_dict["agg_string"] = agg_string
            phenom_list.append(phen_dict)


    return (phenom_list,)

def date2iso(date):
    date = parser.parse(date)
    iso_date = date.isoformat()

    return iso_date

def calculate_md5(filename):
    hash_md5 = hashlib.md5()
    with open(filename, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()