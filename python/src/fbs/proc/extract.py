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
import json
from ceda_elasticsearch_tools.core.log_reader import SpotMapping
from tqdm import tqdm

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

        try:
            handler = self.handler_factory_inst.pick_best_handler(filename)

            if handler is not None:
                handler_inst = handler(filename, level, calculate_md5=calculate_md5) #Can this done within the HandlerPicker class.
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
        if len(hits) > 0 :
            phen_id =  hits[0][u"_source"][u"id"]
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
        if len(fdata) ==  1:
            doc = fdata[0]

        if len(fdata) > 1:
            doc = fdata[0]

            if fdata[1] is not None:
                doc["info"]["phenomena"] = fdata[1]

            if len(fdata) == 3:
                if fdata[2] is not None:
                    doc["info"]["spatial"] = fdata[2]

        return json.dumps(doc)

    def create_bulk_index_json(self, file_list, level, blocksize):
        """
        Creates the JSON required for the bulk index operation. Also produces an array of files which directly match
        the index JSON. This is to get around any problems caused by files with properties errors which produces None
        when self.process_file_seq is called.

        :param file_list: List of files to create actions from
        :param level: Level of detail to get from file
        :param blocksize: Size of chunks to send to Elasticsearch.

        :return: bulk_list - list of JSON strings to send to ES,
                 files_to_index - list of lists with each inner list containing the matching files to the query.
        """
        bulk_json = ""
        bulk_list = []
        files_to_index = []
        file_array = []

        self.logger.debug("Creating bulk json with block of %d" % blocksize)

        for i, filename in enumerate(file_list,1):

            start = datetime.datetime.now()
            doc = self.process_file_seq(filename, level)

            if doc is not None:
                # Get spot information
                spot = self.spots.get_spot(filename)

                es_id = hashlib.sha1(str(filename)).hexdigest()

                # Add spot to level1 info
                if spot is not None:
                    doc[0]['info']['spot_name'] = spot


                action = json.dumps({"index": {"_index": self.es_index, "_id": es_id }}) + "\n"
                body = self.create_body(doc) + "\n"
                self.logger.debug("JSON to index: {}".format(body))

                bulk_json += action + body
                file_array.append(filename)

            else:
                end = datetime.datetime.now()
                self.logger.error("%s|%s|%s|%s ms" % (os.path.basename(filename), os.path.dirname(filename),self.FILE_PROPERTIES_ERROR, str(end - start)))
                self.files_properties_errors = self.files_properties_errors + 1

            if i % blocksize == 0:
                json_len =  bulk_json.count("\n")/2
                self.logger.debug("Loop index(1 based index): %i Files scanned: %i Files unable to scan: %i Blocksize: %i" % (i,json_len,(blocksize-json_len),blocksize))

                # Only attempt to add if there is data there. Will break the scan if it appends an empty action.
                if json_len > 0:
                    bulk_list.append(bulk_json)
                    files_to_index.append(file_array)

                # Reset building blocks
                bulk_json = ""
                file_array = []

        if bulk_json:
            # Add any remaining files
            bulk_list.append(bulk_json)
            files_to_index.append(file_array)

        return bulk_list, files_to_index

    def bulk_index(self, file_list, level, blocksize):
        """
        Creates the JSON and performs a bulk index operation
        """
        action_list, files_to_index = self.create_bulk_index_json(file_list, level, blocksize)

        for action, files in zip(action_list, files_to_index):
            r = self.es.bulk(body=action,request_timeout=60)
            self.process_response_for_errors(r, files)

    def process_response_for_errors(self, response, files):
        if response['errors']:
            for i, item in enumerate(response['items']):
                if item['index']['status'] not in [200,201]:
                    filename = files[i]
                    error = item['index']['error']
                    ex = ": ".join([error['type'],error['reason']])
                    self.logger.error("Indexing error: %s" % ex)
                    self.logger.error(("%s|%s|%s|%s ms" % (os.path.basename(filename), os.path.dirname(filename), self.FILE_INDEX_ERROR,' ')))
                    self.database_errors += 1

                else:
                    self.files_indexed += 1
        else:
            batch_count = len(files)
            self.files_indexed += batch_count
            self.logger.debug("Added %i files to index" % batch_count)

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
            if te[0] == 400:
                pass
            else:
                raise TransportError(te)

        doc = {}
        level = self.conf("level")

        self.logger.debug("File list contains {} files.".format(len(self.file_list)))
        if len(self.file_list) > 0:

            self.bulk_index(self.file_list, level, self.blocksize)

            # At the end print some statistical info.
            logging.getLogger().setLevel(logging.INFO)
            self.logger.info("Summary information for Dataset id : %s, files indexed : %s, database errors : %s,"
                             " properties errors : %s, total files : %s "
                             % ( self.dataset_id, str(self.files_indexed), str(self.database_errors),
                             str(self.files_properties_errors), str(self.total_number_of_files)))

    def prepare_logging_sdf(self):
        """
        Initializes  logging.
        """
        # Check if logging directory exists and if necessary create it.
        log_dir = self.conf("core")["log-path"]

        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        log_fname = "%s_%s_%s.log" % (self.conf("es-configuration")["es-index"], self.conf("dataset"), socket.gethostname())

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

        logging.basicConfig( filename=fpath, filemode="a+", format=log_format, level=level)

        es_log = logging.getLogger("elasticsearch")
        es_log.setLevel(logging.ERROR)
        #es_log.addHandler(logging.FileHandler(fpath_es))

        nappy_log = logging.getLogger("nappy")
        nappy_log.setLevel(logging.ERROR)

        urllib3_log = logging.getLogger("urllib3")
        urllib3_log.setLevel(logging.ERROR)
        #urllib3_log.addHandler(logging.FileHandler(fpath_es))

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
            print( file_to_store_paths)
            try :
                files_written = util.write_list_to_file_nl(self.file_list, file_to_store_paths)
            except Exception as ex:
                self.logger.error("Could not save the python list of files to file...{}".format(ex))
            else:
                self.logger.debug("Paths written in file: {}.".format(files_written))
                self.logger.debug("File {}, containing paths to files in given dataset, has been created.".format(file_to_store_paths))


    def prepare_logging_rdf(self):
        """
        Initializes logging.
        """
        # Check if logging directory exists and if necessary create it.
        log_dir = self.conf("core")["log-path"]

        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # Set log file name
        log_fname = "%s__%s_%s_%s_%s.log" %(self.conf("es-configuration")["es-index"], \
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
        #urllib3_log.addHandler(logging.FileHandler(fpath_es))

        self.logger = logging.getLogger(__name__)

    def read_dataset_from_file_and_scan(self):
        """
        Reads file paths form a given file and returns a subset of them
        in a list.
        """
        #Set up logger and handler class.
        self.prepare_logging_rdf()
        self.logger.debug("***Scanning started.***")
        self.handler_factory_inst = handler_picker.HandlerPicker(self.configuration)
        self.handler_factory_inst.get_configured_handlers()


        file_containing_paths = self.conf("filename")
        start_file = self.conf("start")
        num_of_files = self.conf("num-files")

        self.logger.debug("Copying paths from file {} start is {} and number of lines is {}.".\
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

        #at the end extract metadata.
        self.scan_files()

    #***Functionality for traversing dataset and then immediately extract metadata.***
    def prepare_logging_seq_rs(self):

        """
        initializes  logging.
        """

        #Check if logging directory exists and if necessary create it.
        log_dir = self.conf("core")["log-path"]

        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        #kltsa 15/09/2015 changes for issue :23221.
        log_fname = "%s_%s_%s.log" \
                    %(self.conf("es-configuration")["es-index"],\
                    self.conf("dataset"), socket.gethostname())

        #create the path where to create the log files.
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

        logging.basicConfig( filename=fpath,
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
        #es_log.addHandler(logging.FileHandler(fpath_es))


        nappy_log = logging.getLogger("nappy")
        nappy_log.setLevel(logging.ERROR)


        urllib3_log = logging.getLogger("urllib3")
        urllib3_log.setLevel(logging.ERROR)
        #urllib3_log.addHandler(logging.FileHandler(fpath_es))

        self.logger = logging.getLogger(__name__)

    def read_and_scan_dataset(self):

        self.prepare_logging_seq_rs()
        self.logger.debug("***Scanning started.***.")
        self.handler_factory_inst = handler_picker.HandlerPicker(self.configuration)
        self.handler_factory_inst.get_configured_handlers()

        self.file_list = self.read_dataset()
        self.total_number_of_files = len(self.file_list)
        #at the end extract metadata.
        self.scan_files()
