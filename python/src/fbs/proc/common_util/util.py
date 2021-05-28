"""
Module containing useful functions for the command-line interfaces.
"""

import os
import errno
import sys
import subprocess
import json
import six
import re
import io
import datetime
from dateutil import parser
import hashlib
import logging
import ldap3
from ldap3.core.exceptions import LDAPSessionTerminatedByServerError
from typing import Optional, Union, List
from pwd import getpwuid
from grp import getgrgid

# Python 2/3 compatibility
if sys.version_info.major > 2:
    from configparser import RawConfigParser as ConfigParser
else:
    from ConfigParser import ConfigParser

log_levels = {"debug": logging.DEBUG,
              "info": logging.INFO,
              "warning": logging.WARNING,
              "error": logging.ERROR,
              "critical": logging.CRITICAL
              }

MAX_ATTR_LENGTH = 256

logger = logging.getLogger(__name__)


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
            for key, value in six.iteritems(other_params):
                self.items.append(
                    self.make_param_item(
                        key.strip(),
                        value.strip()
                    )
                )

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


class LotusRunner:
    """
    Class to handle running of tasks using the LOTUS scheduler
    """
    def __init__(self, queue='par-single'):
        self.queue = queue
        self.task_list = []

    def _run_tasks_in_lotus(self) -> None:
        """
        Submit all tasks in task list
        """

        for task in self.task_list:
            self._submit_job(task)

    def _submit_job(self, task: str) -> None:
        """
        Submit the job to LOTUS
        :param task: Task to submit
        """

        if self.queue == 'short-serial':
            wall_time = '24:00:00'
        else:
            wall_time = '48:00:00'

        command = f'sbatch -p {self.queue} -t {wall_time} -e lotus_errors/%j.err {task}'

        print(f'Executing command: {command}')

        subprocess.call(command, shell=True)

    def read_task_file(self, filename: str) -> None:
        """
        Read task file and add to task list
        :param filename: Path to file containing list of tasks
        """
        with open(filename) as reader:
            self.task_list = reader.read().splitlines()

    @staticmethod
    def remove_task_file(filename: str) -> None:
        """
        Delete the task file once the jobs have been submitted

        :param filename: File to remove
        """
        os.remove(filename)

    def run_tasks_in_lotus(self, task_list: list) -> None:
        """
        Run the tasks using the lotus scheduler

        :param task_list: List of tasks to run
        """

        self.task_list = task_list

        self._run_tasks_in_lotus()

    def run_tasks_file_in_lotus(self, filename: str) -> None:
        """
        Load the tasks from file and run in lotus
        scheduler

        :param filename: Path to file containing list of tasks
        """

        self.read_task_file(filename)

        self._run_tasks_in_lotus()

        self.remove_task_file(filename)


class LDAPIdentifier:
    """
    Provides interface to interact with LDAP and get user names
    and group names. The results are cached, as this information
    doesn't change, to reduce load on LDAP.
    """
    def __init__(self, **kwargs):
        """
        :param kwargs: ldap3 Connection kwargs
        """
        self.conn = ldap3.Connection(**kwargs)
        self.users = {}
        self.groups = {}

    def _process_result(self, key: str) -> Optional[str]:
        """
        Process LDAP response object and return the first value for the
        given key.

        :param result: LDAP response object
        :param key: key to return
        :return: value
        """
        if self.conn.entries:
            entry = self.conn.entries[0]
            return getattr(entry, key).value

    def _ldap_query(self, *args, **kwargs) -> None:
        """
        Wraps the LDAP search operation to catch errors
        caused by a closed connection. This method
        restarts the connection and tries the search again.

        :param args: args to pass to ldap3.Connection.search()
        :param kwargs: kwargs to pass to ldap3.Connection.search()
        :return: None
        """
        try:
            self.conn.search(*args, **kwargs)
        except LDAPSessionTerminatedByServerError:
            self.conn.bind()
            self.conn.search(*args, **kwargs)

    def _get_ldap_user(self, uid: Union[str, int]) -> Union[str, int]:
        """
        Get the user listed in LDAP with the given UID

        :param uid: UID to search for
        :return: Username related to UID or UID
        """
        result = uid

        try:
            result = getpwuid(uid).pw_name

        except KeyError:
            self._ldap_query(
                'ou=jasmin,ou=People,o=hpc,dc=rl,dc=ac,dc=uk',
                f'(&(objectClass=posixAccount)(uidNumber={uid}))',
                attributes='uid',
                size_limit=1
            )

            result = self._process_result('uid')

        finally:
            self.users[uid] = result

        return result

    def _get_ldap_group(self, gid: Union[str, int]) -> Union[str, int]:
        """
        Get the group name linked to the given GID
        :param gid: The GID to search for
        :return: Common Name for given GID or GID
        """
        result = gid

        try:
            result = getgrgid(gid).gr_name

        except KeyError:
            self._ldap_query(
                'ou=ceda,ou=Groups,o=hpc,dc=rl,dc=ac,dc=uk',
                f'(&(objectClass=posixGroup)(gidNumber={gid}))',
                attributes='cn',
                size_limit=1
            )

            result = self._process_result('cn')

        finally:
            self.users[gid] = result

        return result

    def get_user(self, uid: Union[str, int]) -> Union[str, int]:
        """
        Either return from the cache, filesystem or search LDAP for the LDAP user
        :param uid: user ID
        :return: Username with UID or UID
        """

        # Try the cache to see if the user ID is stored
        if self.users.get(uid):
            return self.users.get(uid)

        # Try to get the username from the filesystem
        # If the names are not mounted for the uids then
        # query LDAP directly
        return self._get_ldap_user(uid)

    def get_group(self, gid: Union[str, int]) -> Union[str, int]:
        """
        Either return from the cache, filesystem or search LDAP for the LDAP group

        :param gid: group ID
        :return: Groupname with GID or GID
        """

        # Try the cache
        if self.groups.get(gid):
            return self.users.get(gid)

        # Try to get the group name from file system.
        # If names not mounted, query LDAP direct
        return self._get_ldap_group(gid)


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
    for key, value in six.iteritems(config):
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
    config = ConfigParser()
    config.read(filename)

    # get sections
    sections = config.sections()

    conf = {}
    section_options = {}

    for section in sections:

        options = config.options(section)

        for option in options:

            try:
                value = config.get(section, option)
                parsed_value = value.replace("\"", "")
                section_options[option] = parsed_value

                if section_options[option] == -1:
                    section_options[option] = None
            except:
                section_options[option] = None

        conf[section] = section_options.copy()
        section_options.clear()

    return conf


def get_settings(conf_path, args):
    # Default configuration options
    # These are overridden by the config file and command-line arguments
    defaults = {}

    # conf_file = read_conf(conf_path)
    conf_file = cfg_read(conf_path)

    # print( conf_file )

    # Apply updates to CONFIG dictionary in priority order
    # Configuration priority: CONFIG < CONF_FILE < ARGS
    # (CONFIG being lowest, ARGS being highest)
    defaults.update(conf_file)
    defaults.update(args)

    return defaults


def build_file_list(path):
    """
    :param path : A file path
    :return: List of files contained within the specified directory.
    """
    
    file_list = []
    for root, _, files in os.walk(path):
        for each_file in files:
            if each_file[0] == ".": continue
            if os.path.islink(each_file): continue

            file_list.append(os.path.join(root, each_file))

    return file_list


def write_list_to_file(task_list, filename):
    with open(filename, 'w') as writer:
        writer.writelines(
            map(lambda x: x + "\n", task_list)
        )

    return len(task_list)


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

    if value is None:
        return False

    if not is_valid_parameter(key, value):
        return False

    if not is_valid_phen_attr(value):
        return False

    # Returns true if the tests above pass as true
    return True


def is_date_valid(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
        return True
    except ValueError:
        return False


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

            if name in name_filter:
                # Remove extra spaces and any quotation marks which will interfere with creating the agg_string
                value = re.sub('  +', ' ', value).replace('"', '')
                phen_dict[name] = value
                agg_string_list.append('"{}":"{}"'.format(name, value))

            if name in names_list_filter and value not in names:

                # if long_name contains "_" and is the same as standard name, include a version in the names
                # list with the "_" replaced by " "
                if name == "long_name" and long_name_check:
                    value = value.replace("_"," ")

                names.append('"{}"'.format(value))

            phen_dict[name] = value

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