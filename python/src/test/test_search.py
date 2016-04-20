'''
Created on 15 Apr 2016

@author: kleanthis
'''
import sys
import time
from elasticsearch import Elasticsearch
import datetime
sys.path.append(".")


#Details of the database connection.
HOST = "mist.badc.rl.ac.uk"
HOST2 =  "jasmin-es1.ceda.ac.uk"
#HOST = HOST2
PORT = "9200"
INDEX = "archive_level_2_rel" #archive_level_2_1"

#Database object used for communicating with database.
es = Elasticsearch(hosts=[{"host": HOST2, "port": PORT}])

#*******************************************************

def search_database_phenomena(directory):

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

    #get the files.
    res = es.search( index=INDEX,
                     doc_type="file", 
                     body=query,
                     request_timeout=60,
                     size = 10000\
                   )

    hits = res[u'hits'][u'hits']
    phenomena = []

    #get phenomena of each file.
    for item in hits:
        info_dict = item["_source"]["info"]
        if "phenomena" in info_dict:
            file_phenomena = info_dict["phenomena"]
            phenomena += file_phenomena

    phenomena_unique = []

    for item in phenomena:
        if item not in phenomena_unique:
            phenomena_unique.append(item)

    print "Phenomena ids found in directory: " + str(len(phenomena_unique))
    print phenomena_unique

    if len(phenomena_unique) > 0 :

        print "Phenomena attributes:"


        mget_query =\
        {
         "docs":[]
        }

        id_dict = {"_id" : "" }

        for item in phenomena_unique:
            id_dict["_id"] = item
            mget_query["docs"].append(id_dict.copy())

        #get all phenomen with ids found in the second query.
        res = es.mget(body=mget_query, index=INDEX, doc_type='phenomenon')
        count = 0
        for item in res[u'docs']:
            count = count +1
            print str(count) + "." + str(item)


    return phenomena_unique

def main(directory=None):

    if not directory:
        directory = raw_input("Please enter a directory to search for phenomena:")

    start = datetime.datetime.now()
    print "Script started at: %s" %(str(start))

    res = search_database_phenomena(directory)
    #print res

    end = datetime.datetime.now()
    print "Script ended at : %s it ran for : %s" %(str(end), str(end - start))

if __name__ == "__main__":
    dr = None
    if len(sys.argv) > 1:
        dr = sys.argv[1]

    main(dr)