'''
Created on 24 May 2016

@author: kleanthis
'''
"""
Created on 9 May 2016

@author: kleanthis
"""

import sys
import time
from elasticsearch import Elasticsearch
import datetime
import src.fbs.proc.common_util.util as util
import os


def read_cfg():

    c_dir     = os.path.dirname(__file__)
    conf_path = os.path.join(c_dir, "../../../config/ceda_fbs.ini")
    config    = util.cfg_read(conf_path)

    return config

def open_connection(cfg):

    host = cfg["es-configuration"]["es-host"].split(",")[0]
    port = cfg["es-configuration"]["es-port"]
    es_conn = Elasticsearch(hosts=[{"host": host, "port": port}])

    return es_conn

def count_database_docs(cfg):

    query =\
    {
     "query" : 
     {
      "match_all": {}
     }
    }

    #open connection.
    es_conn = open_connection(cfg)



    es_index = cfg["es-configuration"]["es-index"]
    es_type = cfg["es-configuration"]["es-mapping"].split(",")[0]


    res = es_conn.count( index=es_index,
                          doc_type=es_type,
                          body=query,
                          request_timeout=60
                        )

    #{u'count': 27, u'_shards': {u'successful': 5, u'failed': 0, u'total': 5}}
    return res[u"count"]

def get_and_store_stats():

    cfg = read_cfg()

    res = count_database_docs(cfg)

    start_time =  time.strftime("%m-%d %H:%M", time.gmtime())
    record = "Time,%s,files,%s\n" %(str(start_time), res)
    filename =  os.path.join(cfg["core"]["log-path"], "fbs-stats.txt")
    util.save_to_file(filename, record)

def main(directory=None):

    start = datetime.datetime.now()
    print "Script started at: %s" %(str(start))

    get_and_store_stats()

    end = datetime.datetime.now()
    print "Script ended at : %s it ran for : %s" %(str(end), str(end - start))

if __name__ == "__main__":
    main()
