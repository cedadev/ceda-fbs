"""
Usage:
  find_indexed.py --help
  find_indexed.py --version
  find_indexed.py (-d <directory> | --directory <directory>)
                  (-i <index> | --index <index>)

Options:
  --help                                     Show this screen.
  -d --directory=<directory>                 Directory to use.
  -i --index=<index>                         Index to use.
"""

"""
Created on 9 May 2016

@author: kleanthis
"""

import sys
import time
from elasticsearch import Elasticsearch
import datetime
import os
from docopt import docopt

"""
    INPUTS

    index - name of the Index
    directory - location under which all files should be checked 

    OUTPUTS

    output file - containing one full file path per line 

    FUNCTION

    The script should walk the directory structure under the directory provided. 
    For each data file: it should check whether the file has been indexed in the index specified.

    If the file has not been indexed then the full path to the file should be written to the output file specified. 
"""

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

def build_file_list(path):

    file_list = []
    for root, _, files in os.walk(path, followlinks=True):
        for each_file in files:
            file_list.append(os.path.join(root, each_file))

    return file_list

def write_list_to_file(file_list, filename):

    infile = open(filename, 'w')
    items_written = 0

    for item in file_list:
        infile.write("%s" %(item+"\n"))
        items_written += 1

    infile.close()
    return items_written

def open_connection(cfg):

    args = cfg["index"].split("/")


    host = args[0].split(":")[0]
    port = args[0].split(":")[1]

    es_conn = Elasticsearch(hosts=[{"host": host, "port": port}])

    return es_conn

def create_query(filename):

    query =\
    {
     "query":
     {
      "matchPhrase" : { "file.path" : filename}
     }
    }

    #test_query =\
    #{
    # "query":
    # {
    #  "bool" :
    #  {
    #   "must" : { "match" : { "info.name" : os.path.basename(filename) } }
    #  }
    # }
    #}

    return query

def search_database_for_files(cfg):

    #open connection.
    es_conn = open_connection(cfg)
    args = cfg["index"].split("/")

    es_index = args[1]
    es_type = args[2]


    directory = cfg["directory"]

    file_list = build_file_list(directory)
    #file_list.append("/badc/eufar/data/projects/icare-qad/ncar-c130_20101103_icare/core_ncar-c130_20101103_final.nc")

    files_found = len(file_list)
    file_not_found_list = []
    print "Directory: {}".format(directory)
    print "Number of files found: {}".format(files_found)

    files_indexed = 0
    files_not_indexed = 0 

    for item in file_list:
        query = create_query(item)

        res = es_conn.search( index=es_index,
                              doc_type=es_type,
                              body=query,
                              request_timeout=60,
                              size = 10000
                            )

        hits = res[u'hits'][u'hits']

        if len(hits) > 0:
            files_indexed = files_indexed + 1
        else:
            files_not_indexed = files_not_indexed + 1
            file_not_found_list.append(item)


    print "Number of files indexed: {}".format(files_indexed)
    print "Number of files not indexed: {}".format(files_not_indexed)
    write_list_to_file(file_not_found_list, "files_not_found.txt")

def main(directory=None):

    start = datetime.datetime.now()
    print "==============================="
    print "Script started at: %s." %(str(start))


    #Gets command line arguments.
    cfg = sanitise_args(docopt(__doc__))

    search_database_for_files(cfg)


    end = datetime.datetime.now()
    print "Script ended at : %s  it ran for : %s."\
          %(str(end), str(end - start))
    print "==============================="

if __name__ == "__main__":
    main()
