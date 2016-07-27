from datetime import datetime
from copy import deepcopy
import json
import sys
import socket, time
from elasticsearch import Elasticsearch
from content import *
from __builtin__ import file
sys.path.append(".")


#Details of the database connection.
HOST = "mist.badc.rl.ac.uk"
HOST2 = "jasmin-es1.ceda.ac.uk"
#HOST = HOST2
PORT = "9200"
INDEX = "agrel"

#Database object used for communicating with database.
es = Elasticsearch(hosts=[{"host": HOST, "port": PORT}])

#*******************************************************

def remove_index():

    """
    Removes the INDEX so that 
    the use cases can be run again. 
    """

    res = es.indices.delete(index=INDEX)
    time.sleep(1)

def create_index():

    """
    Creates the INDEX in database. 
    """

    res = es.indices.create(index=INDEX, body=mapping)
    time.sleep(2)

def get_hash(fname):
    return abs(hash((socket.gethostname() + str(time.time()))))

def add_files():

    """
    Adds files to the INDEX.
    """
    ids = []
    for i in range(0, 700):
        fname = "file_%02d" % i
        doc = get_file_doc(fname)
        id = abs(get_hash(fname))
        es.index(index=INDEX, doc_type="file", id=id, body=doc)
        ids.append(id)
        time.sleep(0.1)

    return ids

def add_phenomena():

    """
    Adds a basic list of phenomena to INDEX.
    """
    ids = []
    chars = "abcdefg"
    for i in range(4):
        phen = get_phenomenon(i)
        id = abs(hash(str(phen)))
        phen["id"] = id
        es.index(index=INDEX, doc_type="phenomenon", id=id, body=phen)
        ids.append(id)

    return ids

def update_files(fids, pids):

    """
    Part of the use case where the phenomena of files
    need to be updated.

    #Files 000-099: rainfall
    #Files 100-199: temperature
    #Files 200-299: pressure
    #Files 300-399: pressure subset
    #Files 400-499: temperature, rainfall
    #Files 500-599: temperature, rainfall, pressure
    #Files 600-699: temperature, rainfall, pressure subset
    """

    body_l= {}
    count = 0
    for item in fids:
        fname = "file_%02d" % 1
        doc = get_file_doc(fname)

        if count <= 99:
            doc["info"]["phenomena"] = pids[0]
        elif count >= 100 and count <= 199:
            doc["info"]["phenomena"] = pids[1]
        elif count >= 200 and count <= 299:
            doc["info"]["phenomena"] = pids[2]
        elif count >= 300 and count <= 399:
            doc["info"]["phenomena"] = pids[3]
        elif count >= 400 and count <= 499:
            tmp = pids[0:2]
            #print tmp
            doc["info"]["phenomena"] = tmp
        elif count >= 500 and count <= 599:
            tmp = pids[0:3]
            #print tmp
            doc["info"]["phenomena"] = tmp
        elif count >= 600 and count <= 699:
            tmp = [pids[0:2], pids[3]]
            #print tmp
            doc["info"]["phenomena"] = tmp

        body_l["doc"] = doc
        es.update(index=INDEX, doc_type="file", id=item, body=body_l)
        count = count + 1
        print count

bulk_requests = []
def index_phenomenon(phenomenon = None, threshold=0):

    """
    Indexes a phenomenon of a file.
    Returns the id that the phenomenon will have in the database.
    """

    pid = None
    json_query = ""
    global bulk_requests

    if phenomenon is not None:
        pid = abs(hash(str(phenomenon)))
        phenomenon["id"] = pid
        index  =  { "index": {"_id": "" }} 
        index["index"]["_id"] = pid

        json_query = json.dumps(index) + "\n"
        json_query = json_query + json.dumps(phenomenon) + "\n"
        bulk_requests.append(json_query)

    if len(bulk_requests) > threshold:

        print "indexing "  + str(len(bulk_requests)) + " phenomena."

        for item in bulk_requests:
            json_query = json_query + item

        bulk_requests = []

        try:
            es.bulk(index=INDEX, doc_type="phenomenon", body=json_query)
            #print "Phenomenon saved in database."
            time.sleep(1) # Make sure that thi sis submitted before we query again the database.
        except Exception as ex:
            print ex

    return pid

def index_file(fid, fjson):

    """Indexes a file. """

    es.index(index=INDEX, doc_type="file", id=fid, body=fjson)
    time.sleep(0.01)

def search_database(query):

    """
    Executes a DSL query and returns the result.
    A delay is used because high rate of queries cause the 
    database to return an error.
    """

    res = es.search( index=INDEX,
                     doc_type="phenomenon", 
                     body=query
                   )
    time.sleep(0.1)
    return res

def create_query(phenomenon):

    """
    Returns a DSL query that searches for the a given phenomenon.
    """

    es_subquery_name_template =\
    {
     "match_phrase": { "attributes.name" : "phenomenon attr2"  }
    }

    es_subquery_value_template =\
    {
     "match_phrase": { "attributes.value" : "phenomenon attr2"  }
    }

    es_subquery_count =\
    {
     "match": { "attribute_count" : "3" }
    }

    es_subquery_template =\
    {
     "nested": 
     {
      "path": "attributes", 
      "query":
      {
       "bool": 
       {
        "must": [ ]
       }
      }
     }
    }

    #Basic query template.
    es_query_template =\
    {
     "query": 
     {
      "bool": 
      {
       "must": [ ]
      }
     }
    }

    attributes = phenomenon["attributes"]
    number_of_attributes = 0
    for item in attributes:
        name = True
        es_subquery_template_copy = deepcopy(es_subquery_template)
        for key in item:
            if name :
                es_subquery_name_template_copy  = deepcopy(es_subquery_name_template)
                es_subquery_name_template_copy["match_phrase"]["attributes.name"] = item[key]
                es_subquery_template_copy["nested"]["query"]["bool"]["must"].append(es_subquery_name_template_copy)
                name = False
                number_of_attributes = number_of_attributes +1
            else:
                es_subquery_value_template_copy = deepcopy(es_subquery_value_template)
                es_subquery_value_template_copy["match_phrase"]["attributes.value"] = item[key]
                es_subquery_template_copy["nested"]["query"]["bool"]["must"].append(es_subquery_value_template_copy)
                name = True

        es_query_template["query"]["bool"]["must"].append(es_subquery_template_copy)

    #es_subquery_count_copy = deepcopy(es_subquery_count)
    es_subquery_count["match"]["attribute_count"] = number_of_attributes
    es_query_template["query"]["bool"]["must"].append(es_subquery_count)

    return es_query_template

#*******************************************************

def is_valid_result(result):

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

def netcdf_file_handler(index):

    """
    Returns file info and phenomena information of a NETCDF file.
    """

    fname = "file_%02d" % index
    fmeta = get_file_doc(fname)
    #fphen = get_file_phenomena()
    fphen = get_file_phenomena_i(index)
    return (fmeta, fphen)

def simulate_indexing_of_files():

    """
    Implements the following scenario:
    1. Metadata are extracted for a file (file info and phenomena).
    2. If phenomena do not exist in database then they are created.
    3. Phenomena ids are stored in the json representing file info.
    4. File info is stored in database.
    5. This is done for all files in the list. Current size is 700.
    """

    total_phen_found = 0
    total_phen_new = 0

    for i in range(0, 700):

        #Extract file info and phenomena from file.
        metadata = netcdf_file_handler(i)
        fmeta = metadata[0]
        phen_list = metadata[1]
        fid = abs(get_hash(fmeta))

        phen_ids = []
        #Test if phenomenon exist in database.
        #if not create it.
        for item in phen_list:

            query = create_query(item)
            print "Query created: " + str(query)
            res = search_database(query)
            print "Query result: " + str(res)

            phen_id = is_valid_result(res)
            if phen_id is not None:
                phen_ids.append(phen_id)
                total_phen_found = total_phen_found + 1
                #Record the id and then index he file
            else:
                #print "phenomenon needs to be inserted in the database."
                phen_id = index_phenomenon(item, 800)
                phen_ids.append(phen_id)
                total_phen_new = total_phen_new +1
                print "Phen created : " + str(phen_id)
                #print phen_id

        index_phenomenon()
        #if wait_init:
        #    time.sleep(1)
        #    wait_init = False

        fmeta["info"]["phenomena"] = phen_ids
        index_file(fid, fmeta)

    print "phenomena created : " + str(total_phen_new)
    print "phenomenon found : " + str(total_phen_found)

def main():
    try:
        remove_index()
        print "INDEX REMOVED."
    except:
        pass

    print "CREATING INDEX."
    create_index()
    #print "ADDING FILES."
    #file_ids = add_files()
    #print "CREATING PHENS."
    #phen_ids = add_phenomena()

    #print "UPDATING FILES."
    #update_files(file_ids, phen_ids)

    print "RUNNING USE CASE."
    simulate_indexing_of_files()

if __name__ == "__main__":

    main()
