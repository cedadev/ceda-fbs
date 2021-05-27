"""
'Extract' module - handles file crawling and metadata extraction.
"""

import datetime
import logging
import os
import hashlib
import socket
import fbs.proc.common_util.util as util
import fbs.proc.file_handlers.handler_picker as handler_picker
from elasticsearch.exceptions import TransportError
from es_iface.factory import ElasticsearchClientFactory
from es_iface import index
from ceda_elasticsearch_tools.core.log_reader import SpotMapping
from elasticsearch.helpers import bulk

# Suppress requests logging messages
logging.getLogger("requests").setLevel(logging.WARNING)


class ExtractSeq(object):
    """
    File crawler and metadata extractor class.
    Part of core functionality of FBS.
    Files are scanned sequentially (one thread).
    """

    def __init__(self, conf):

        self.configuration = conf
        self.logger = None
        self.handler_factory_inst = None
        self.file_list = []

        self.es = None
        self.dataset_id = None
        self.dataset_dir = None

        # Spot data
        self.spots = SpotMapping(spot_file='ceda_all_datasets.ini')

        # LDAP lookup
        ldap_hosts = self.conf('ldap-configuration')['hosts'].split(',')
        self.ldap_interface = util.LDAPIdentifier(ldap_hosts)

        # Define constants
        self.blocksize = 800
        self.FILE_PROPERTIES_ERROR = "0"
        self.FILE_INDEX_ERROR = "-1"
        self.FILE_INDEXED = "1"

        # Variables for storing statistical information.
        self.database_errors = 0
        self.files_properties_errors = 0
        self.files_indexed = 0
        self.total_number_of_files = 0

        # Database connection information.
        self.es_index = self.conf("es-configuration")["es-index"]

    # General purpose methods
    def conf(self, conf_opt):
        """
        Return configuration option or raise exception if it doesn't exist.
        :param str conf_opt: The name of the configuration option to find.
        """
        if conf_opt in self.configuration:
            return self.configuration[conf_opt]
        else:
            raise AttributeError(
                "Mandatory configuration option not found: %s" % conf_opt)

    def read_dataset(self):
        """
        Returns the files contained within a dataset.
        """

        datasets_file = self.conf("filename")
        self.dataset_id = self.conf("dataset")

        # directory where the files to be searched are.
        self.dataset_dir = util.find_dataset(datasets_file, self.dataset_id)
        self.logger.debug(f'Datset directory: {self.dataset_dir}')
        print(f'Datset directory: {self.dataset_dir}')

        if self.dataset_dir is not None:
            self.logger.debug("Scannning files in directory {}.".format(self.dataset_dir))
            return util.build_file_list(self.dataset_dir)
        else:
            return None

    def process_file_seq(self, filename, level):
        """
        Returns metadata from the given file.
        """
        calculate_md5 = self.conf("calculate_md5")
        if not os.path.isfile(filename):
            self.logger.error("{} Is not a file.".format(filename))
            return None

        try:
            handler = self.handler_factory_inst.pick_best_handler(filename)

            if handler is not None:
                handler_inst = handler(filename, level,
                                       calculate_md5=calculate_md5)  # Can this done within the HandlerPicker class.
                metadata = handler_inst.get_metadata()
                self.logger.debug("{} was read using handler {}.".format(filename, handler_inst.handler_id))
                return metadata

            else:
                self.logger.error("{} could not be read by any handler.".format(filename))
                return None

        except Exception as ex:
            self.logger.error("Could not process file: {}".format(ex))

    def is_valid_result(self, result):

        """
        Validates the result of a DSL query by analizing the 
        hits list. 
        """

        hits = result[u'hits'][u'hits']
        if len(hits) > 0:
            phen_id = hits[0][u"_source"][u"id"]
            return phen_id
        else:
            return None

    @staticmethod
    def create_body(fdata):
        """
        Takes the information returned by the file handlers and builds the JSON to send to elasticsearch.

        :param fdata: Tuple containing file metatadata, parameters and temporal data.
        :return: JSON to index  into elasticsearch
        """
        doc = {}

        if len(fdata) == 1:
            doc = fdata[0]

        if len(fdata) > 1:
            doc = fdata[0]

            if fdata[1] is not None:
                doc["info"]["phenomena"] = fdata[1]

            if len(fdata) == 3:
                if fdata[2] is not None:
                    doc["info"]["spatial"] = fdata[2]

        return doc

    def _generate_action_list(self, file_list, level):

        self.logger.debug("Bulk indexing results")
        start = datetime.datetime.now()

        for file in file_list:

            metadata = self.process_file_seq(file, level)

            if metadata is not None:

                # Get spot info
                spot = self.spots.get_spot(file)

                es_id = hashlib.sha1(str(file).encode('utf-8')).hexdigest()

                body = self.create_body(metadata)

                # Add spot to level1 metadata
                if spot is not None:
                    body['info']['spot_name'] = spot

                uid = body['info']['user']
                gid = body['info']['group']

                body['info']['user'] = self.ldap_interface.get_user(uid)
                body['info']['group'] = self.ldap_interface.get_group(gid)

                doc = {
                    '_index': self.es_index,
                    '_id': es_id,
                    '_source': body
                }
                yield doc

            else:
                end = datetime.datetime.now()
                self.logger.error("%s|%s|%s|%s ms" % (
                os.path.basename(file), os.path.dirname(file), self.FILE_PROPERTIES_ERROR, str(end - start)))
                self.files_properties_errors = self.files_properties_errors + 1

    def bulk_index(self, file_list, level):
        """
        Scan the files and index them
        :param file_list: File list to operate on
        :param level: Level of detail to retrieve
        :return:
        """

        bulk(self.es, self._generate_action_list(file_list, level))

    def scan_files(self):
        """
        Extracts metadata information from files and posts them in elastic search.
        """
        # Sanity check.
        if self.file_list is None:
            self.logger.info("File list is empty.")
            return

        # Create index if necessary
        self.logger.debug("Setting elastic search index.")
        es_factory = ElasticsearchClientFactory()
        self.es = es_factory.get_client(self.configuration)

        try:
            index.create_index(self.configuration, self.es)
        except TransportError as te:
            if te.status_code == 400:
                pass
            else:
                raise TransportError(te)

        level = self.conf("level")

        self.logger.debug("File list contains {} files.".format(len(self.file_list)))
        if len(self.file_list) > 0:
            self.bulk_index(self.file_list, level)

            # At the end print some statistical info.
            logging.getLogger().setLevel(logging.INFO)
            self.logger.info("Summary information for Dataset id : %s, files indexed : %s, database errors : %s,"
                             " properties errors : %s, total files : %s "
                             % (self.dataset_id, str(self.files_indexed), str(self.database_errors),
                                str(self.files_properties_errors), str(self.total_number_of_files)))

    def prepare_logging_sdf(self):
        """
        Initializes  logging.
        """
        # Check if logging directory exists and if necessary create it.
        log_dir = self.conf("core")["log-path"]

        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        log_fname = "%s_%s_%s.log" % (
        self.conf("es-configuration")["es-index"], self.conf("dataset"), socket.gethostname())

        # create the path where to create the log files.
        fpath = os.path.join(log_dir, log_fname)

        conf_log_level = self.conf("core")["log-level"]

        log_format = self.conf("core")["format"]
        level = util.log_levels.get(conf_log_level, logging.NOTSET)

        """
        ok, since this is the main module lets remove previously configured handlers
        and add the one used in this script.
        """
        logging.root.handlers = []

        logging.basicConfig(filename=fpath, filemode="a+", format=log_format, level=level)

        es_log = logging.getLogger("elasticsearch")
        es_log.setLevel(logging.ERROR)

        nappy_log = logging.getLogger("nappy")
        nappy_log.setLevel(logging.ERROR)

        urllib3_log = logging.getLogger("urllib3")
        urllib3_log.setLevel(logging.ERROR)

        self.logger = logging.getLogger(__name__)

    def store_dataset_to_file(self):
        """
        Stores filenames of files within a dataset to a file.
        """
        self.prepare_logging_sdf()
        self.logger.debug("***Scanning started.***")
        self.file_list = self.read_dataset()

        if self.file_list is not None:
            file_to_store_paths = self.conf("make-list")

            try:
                files_written = util.write_list_to_file(self.file_list, file_to_store_paths)
            except Exception as ex:
                self.logger.error("Could not save the python list of files to file...{}".format(ex))
            else:
                self.logger.debug("Paths written in file: {}.".format(files_written))
                self.logger.debug("File {}, containing paths to files in given dataset, has been created.".format(
                    file_to_store_paths))

    def prepare_logging_rdf(self):
        """
        Initializes logging.
        """
        # Check if logging directory exists and if necessary create it.
        log_dir = self.conf("core")["log-path"]

        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # Set log file name
        log_fname = "%s__%s_%s_%s_%s.log" % (self.conf("es-configuration")["es-index"], \
                                             os.path.basename(self.conf("filename")), \
                                             self.conf("start"), \
                                             self.conf("num-files"), \
                                             socket.gethostname())

        # Create the path where to create the log files.
        fpath = os.path.join(log_dir, log_fname)

        conf_log_level = self.conf("core")["log-level"]
        log_format = self.conf("core")["format"]
        level = util.log_levels.get(conf_log_level, logging.NOTSET)

        logging.root.handlers = []
        logging.basicConfig(filename=fpath, filemode="a+", format=log_format, level=level)

        es_log = logging.getLogger("elasticsearch")
        es_log.setLevel(logging.ERROR)

        nappy_log = logging.getLogger("nappy")
        nappy_log.setLevel(logging.ERROR)

        urllib3_log = logging.getLogger("urllib3")
        urllib3_log.setLevel(logging.ERROR)
        # urllib3_log.addHandler(logging.FileHandler(fpath_es))

        self.logger = logging.getLogger(__name__)

    def read_dataset_from_file_and_scan(self):
        """
        Reads file paths form a given file and returns a subset of them
        in a list.
        """
        # Set up logger and handler class.
        self.prepare_logging_rdf()
        self.logger.debug("***Scanning started.***")
        self.handler_factory_inst = handler_picker.HandlerPicker()

        file_containing_paths = self.conf("filename")
        start_file = self.conf("start")
        num_of_files = self.conf("num-files")

        self.logger.debug("Copying paths from file {} start is {} and number of lines is {}.". \
                          format(file_containing_paths, start_file, num_of_files))

        filename = os.path.basename(file_containing_paths)
        self.dataset_id = os.path.splitext(filename)[0]
        self.logger.debug("Dataset id is  {}.".format(self.dataset_id))

        content = util.read_file_into_list(file_containing_paths)

        self.total_number_of_files = len(content)
        self.logger.debug("{} lines read from file {}.".format((len(content)), file_containing_paths))

        if int(start_file) < 0 or int(start_file) > self.total_number_of_files:
            self.logger.error("Please correct start parameter value.")
            return

        end_file = int(start_file) + int(num_of_files)
        if end_file > self.total_number_of_files:
            self.logger.error("Please correct num-files parameter value because it is out of range.")
            return

        file_list = content[int(start_file):end_file]
        content = None

        self.logger.debug("{} files copied in local file list.".format(len(file_list)))

        for path in file_list:
            self.file_list.append(path.rstrip())

        # at the end extract metadata.
        self.scan_files()

    # Functionality for traversing dataset and then immediately extract metadata.
    def prepare_logging_seq_rs(self):
        """
        initializes  logging.
        """

        # Check if logging directory exists and if necessary create it.
        log_dir = self.conf("core")["log-path"]

        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        log_fname = "%s_%s_%s.log" \
                    % (self.conf("es-configuration")["es-index"], \
                       self.conf("dataset"), socket.gethostname())

        # create the path where to create the log files.
        fpath = os.path.join(log_dir,
                             log_fname
                             )

        conf_log_level = self.conf("core")["log-level"]

        log_format = self.conf("core")["format"]
        level = util.log_levels.get(conf_log_level, logging.NOTSET)

        """
        ok, since this is the main module lets remove previously configured handlers
        and add the one used in this script.
        """
        logging.root.handlers = []

        logging.basicConfig(filename=fpath,
                            filemode="a+",
                            format=log_format,
                            level=level
                            )
        """
        extract_logger = logging.getLogger(__name__)

        file_handler = logging.FileHandler(fpath)
        log_format = logging.Formatter(log_format)
        file_handler.setFormatter(log_format)

        extract_logger.addHandler(file_handler)
        extract_logger.setLevel(level)
        extract_logger.propagate = 0
        """

        es_log = logging.getLogger("elasticsearch")
        es_log.setLevel(logging.ERROR)

        nappy_log = logging.getLogger("nappy")
        nappy_log.setLevel(logging.ERROR)

        urllib3_log = logging.getLogger("urllib3")
        urllib3_log.setLevel(logging.ERROR)

        self.logger = logging.getLogger(__name__)

    def read_and_scan_dataset(self):

        self.prepare_logging_seq_rs()
        self.logger.debug("***Scanning started.***.")
        self.handler_factory_inst = handler_picker.HandlerPicker()

        self.file_list = self.read_dataset()
        self.total_number_of_files = len(self.file_list)

        # Extract metadata.
        self.scan_files()
