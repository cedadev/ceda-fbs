"""
Created on 9 May 2016

@author: kleanthis
"""

import sys
import time
from elasticsearch import Elasticsearch
import datetime
import fbs_lib.util as util
import os

"""
INPUTS:
    directory: a string representing a full directory path.

OUTPUTS:
    A dictionary containing the following key/value pairs:
    "phenomena": a list of phenomena dictionaries
    "size": the overall size (in bytes) of the directory calculated as the sum of all file sizes under that directory tree
    "number_of_files": the total number of files under that directory tree
    "file_format": a unique list of all file formats found under that directory tree.
"""

HOST = "mist.badc.rl.ac.uk"
HOST2 =  "jasmin-es1.ceda.ac.uk"
#HOST = HOST2
PORT = "9200"
INDEX = "archive_level_2_rel" #archive_level_2_1"

def read_cfg():

    c_dir     = os.path.dirname(__file__)
    conf_path = os.path.join(c_dir, "../config/ceda_fbs.ini")
    config    = util.cfg_read(conf_path)

    return config["es-configuration"]

def open_connection(cfg):

    host = cfg["es-host"]
    port = cfg["es-port"]
    es_conn = Elasticsearch(hosts=[{"host": host, "port": port}])

    return es_conn

#*******************************************************

def search_database_phenomena_real(cfg, directory):

    """
    searches for phenomena under certain directory.
    """

    query =\
    {
     "query" : 
     {
      "wildcard" : 
      {
       "info.directory" : directory + "*"
      }
     }
    }

    #open connection.
    es_conn = open_connection(cfg)

    es_index = cfg["es-index"]
    es_type = cfg["es-mapping"].split(",")[0]

    #get the files.
    res = es_conn.search( index=es_index,
                          doc_type=es_type,
                          body=query,
                          request_timeout=60,
                          size = 10000\
                        )

    hits = res[u'hits'][u'hits']
    phenomena = []
    formats = []
    number_of_files = len(hits)
    #get phenomena of each file.
    total_size = 0
    for item in hits:
        info_dict = item["_source"]["info"]
        if "phenomena" in info_dict:
            file_phenomena = info_dict["phenomena"]
            phenomena += file_phenomena

        total_size += info_dict[u'size']
        if u"format" in info_dict:
            formats.append(info_dict[u'format'])

    phenomena_unique = []
    formats_unique = []

    for item in phenomena:
        if item not in phenomena_unique:
            phenomena_unique.append(item)

    for item in formats:
        if item not in formats_unique:
            formats_unique.append(item)

    print "Phenomena ids found in directory: " + str(len(phenomena_unique))
    print phenomena_unique

    es_type = cfg["es-mapping"].split(",")[1]

    if len(phenomena_unique) > 0 :

        mget_query =\
        {
         "docs":[]
        }

        id_dict = {"_id" : "" }

        for item in phenomena_unique:
            id_dict["_id"] = item
            mget_query["docs"].append(id_dict.copy())

        #get all phenomen with ids found in the second query.
        res = es_conn.mget(body=mget_query, index=es_index, doc_type=es_type)

        #for item in res[u'docs']:
        #    count = count +1
        #    print str(count) + "." + str(item)


    summary_info = {}
    summary_info["number_of_files"] = number_of_files
    summary_info["total_size"] = total_size
    summary_info["formats"] = formats_unique
    summary_info[" phenomena"] = res[u'docs']

    return summary_info

def search_database_phenomena(directory):

    cfg = read_cfg()

    res = search_database_phenomena_real(cfg, directory)

    return res

def main(directory=None):

    if not directory:
        directory = raw_input("Please enter a directory to search for phenomena:")

    start = datetime.datetime.now()
    print "Script started at: %s" %(str(start))

    res = search_database_phenomena(directory)
    for x in res:
        print x
        print res[x]

    end = datetime.datetime.now()
    print "Script ended at : %s it ran for : %s" %(str(end), str(end - start))

if __name__ == "__main__":
    dr = None
    if len(sys.argv) > 1:
        dr = sys.argv[1]

    main(dr)