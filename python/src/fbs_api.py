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
    "sample_names" sample filenames found in directory. 
"""

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

def search_database_phenomena(cfg, directory):

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

    fname_sample = 0
    fname_sample_list = []
    last_file_type = ""

    #open connection.
    es_conn = open_connection(cfg)

    es_index = cfg["es-index"]
    es_type = cfg["es-mapping"].split(",")[0]


    res = es_conn.search( index=es_index,
                          doc_type=es_type,
                          body=query,
                          request_timeout=60,
                          size = 10000,
                          scroll = '5m',
                          search_type = 'scan',
                        )

    sid = res['_scroll_id']
    scroll_size = res['hits']['total']

    phenomena = []
    formats = []
    total_size = 0
    number_of_files = 0
    valid_formats = [".nc",".na",".pp",".grb",".grib",".GRB",".GRIB",".manifest",".json",".kmz",".kml",".csv",".hdf"]

    # Start scrolling
    while (scroll_size > 0):
        #print "Scrolling..."
        page = es_conn.scroll(scroll_id = sid, scroll = '5m')
        # Update the scroll ID
        sid = page['_scroll_id']

        hits = page[u'hits'][u'hits']
        # Get the number of results that we returned in the last scroll.
        scroll_size = len(hits)
        number_of_files += scroll_size

        #get phenomena of each file.
        for item in hits:
            info_dict = item["_source"]["info"]
            if u"phenomena" in info_dict:
                file_phenomena = info_dict[u"phenomena"]
                for item in file_phenomena:
                    if item not in phenomena:
                        phenomena.append(item)

            if u"size" in info_dict:
                total_size += info_dict[u'size']

            if u"format" in info_dict:
                if info_dict[u'format'] not in formats:
                    formats.append(info_dict[u'format'])

            if u"name" in info_dict:
                if fname_sample < 2:
                    file_name = info_dict[u"name"]
                    current_file_type = os.path.splitext(file_name)[1]
                    if current_file_type is not None:
                        if current_file_type in valid_formats:
                            if last_file_type != current_file_type:
                                fname_sample_list.append(info_dict[u"name"])
                                fname_sample += 1
                                last_file_type = current_file_type

    es_type = cfg["es-mapping"].split(",")[1]

    if len(phenomena) > 0 :

        mget_query =\
        {
         "docs":[]
        }

        id_dict = {"_id" : "" }

        for item in phenomena:
            id_dict["_id"] = item
            mget_query["docs"].append(id_dict.copy())

        #get all phenomen with ids found in the first query.
        res = es_conn.mget(body=mget_query, index=es_index, doc_type=es_type)
    else:
        res = {'docs': []}

    summary_info = {}
    summary_info["number_of_files"] = number_of_files
    summary_info["total_size"] = total_size
    summary_info["formats"] = formats
    summary_info["phenomena"] = res[u'docs']
    summary_info["sample_names"] = fname_sample_list

    return summary_info

def get_dir_info(directory):

    cfg = read_cfg()

    res = search_database_phenomena(cfg, directory)

    return res

def main(directory=None):

    if not directory:
        directory = raw_input("Please enter a directory to search for phenomena:")

    start = datetime.datetime.now()
    print "Script started at: %s" %(str(start))

    res = get_dir_info(directory)
    for item in res:
        print item
        print res[item]

    end = datetime.datetime.now()
    print "Script ended at : %s it ran for : %s" %(str(end), str(end - start))

if __name__ == "__main__":
    dr = None
    if len(sys.argv) > 1:
        dr = sys.argv[1]

    main(dr)
