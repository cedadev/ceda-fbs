from datetime import datetime
from copy import deepcopy
import json


import sys
sys.path.append(".")
import socket, time

from elasticsearch import Elasticsearch

from content import *

HOST = "mist.badc.rl.ac.uk"
HOST2 = "jasmin-es1.ceda.ac.uk"
#HOST = HOST2
PORT = "9200"
INDEX = "agrel"
es = Elasticsearch(hosts=[{"host": HOST, "port": PORT}])

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
    "must": 
    [ ]
   }
  }
 }
}

es_query_template =\
{
 "query": 
 {
  "bool": 
  {
   "must": 
   [ ]
  }
 }
}

def remove_index():
    res = es.indices.delete(index=INDEX)

def create_index():
    res = es.indices.create(index=INDEX, body=mapping)

def get_hash(fname):
    return abs(hash((socket.gethostname() + str(time.time()))))

def add_files():
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
    ids = []
    chars = "abcdefg"
    for i in range(4):
        phen = get_phenomenon(i)
        id = abs(hash(str(phen)))
        phen["id"] = id
        es.index(index=INDEX, doc_type="phenomenon", id=id, body=phen)
        ids.append(id)

    return ids

def update_files(file_ids, phen_ids):
    #Files 000-099: rainfall
    #Files 100-199: temperature
    #Files 200-299: pressure
    #Files 300-399: pressure subset
    #Files 400-499: temperature, rainfall
    #Files 500-599: temperature, rainfall, pressure
    #Files 600-699: temperature, rainfall, pressure subset

    body_l= {}
    count = 0
    for item in file_ids:
        fname = "file_%02d" % 1
        doc = get_file_doc(fname)

        if count <= 99:
            doc["info"]["phenomena"] = phen_ids[0]
        elif count >= 100 and count <= 199:
            doc["info"]["phenomena"] = phen_ids[1]
        elif count >= 200 and count <= 299:
            doc["info"]["phenomena"] = phen_ids[2]
        elif count >= 300 and count <= 399:
            doc["info"]["phenomena"] = phen_ids[3]
        elif count >= 400 and count <= 499:
            tmp = phen_ids[0:2]
            #print tmp
            doc["info"]["phenomena"] = tmp
        elif count >= 500 and count <= 599:
            tmp = phen_ids[0:3]
            #print tmp
            doc["info"]["phenomena"] = tmp
        elif count >= 600 and count <= 699:
            tmp = [phen_ids[0:2], phen_ids[3]]
            #print tmp
            doc["info"]["phenomena"] = tmp

        body_l["doc"] = doc
        es.update(index=INDEX, doc_type="file", id=item, body=body_l)
        count = count + 1
        print count

def search_elasticsearch(query):
    res = es.search( index=INDEX,
                     doc_type="phenomenon", 
                     body=query
                   )
    return res

def is_valid_result(result):

    hits = result[u'hits'][u'hits']
    if len(hits) > 0 :
        phen_id =  hits[0][u"_source"][u"id"]
        return phen_id
    else:
        return None

def create_query(phenomenon):
    #format of phenomenon
    #{'attributes': [{'name': 'value'}], 'attribute_count': '1', 'id': '2017'}
    attributes = phenomenon["attributes"]
    number_of_attributes = 0
    for item in attributes:
        es_query_template_copy = deepcopy(es_query_template)
        name = True
        es_subquery_template_copy = deepcopy(es_subquery_template)
        for key in item:
            if name :
                es_subquery_name_template_copy  = deepcopy(es_subquery_name_template)
                es_subquery_name_template_copy["match_phrase"]["attributes.name"] = item[key]
                es_subquery_template_copy["nested"]["query"]["bool"]["must"].append(es_subquery_name_template_copy)
                name = False
            else:
                es_subquery_value_template_copy = deepcopy(es_subquery_value_template)
                es_subquery_value_template_copy["match_phrase"]["attributes.value"] = item[key]
                es_subquery_template_copy["nested"]["query"]["bool"]["must"].append(es_subquery_value_template_copy)
                name = True


        es_query_template_copy["query"]["bool"]["must"].append(es_subquery_template_copy)
        number_of_attributes = number_of_attributes +1
    es_subquery_count_copy = deepcopy(es_subquery_count)
    es_subquery_count_copy["match"]["attribute_count"] = number_of_attributes
    es_query_template_copy["query"]["bool"]["must"].append(es_subquery_count_copy)

    return es_query_template_copy

def add_phenomenon(phenomenon):
    id = abs(hash(str(phenomenon)))
    phenomenon["id"] = id
    index  =  { "index": {"_id": "" }} 
    index["index"]["_id"] = id

    body = json.dumps(index) + "\n"
    body = body + json.dumps(phenomenon) + "\n"

    es.bulk(index=INDEX, doc_type="phenomenon", body=body)

def update_phenomena():
    file_phen = get_file_phenomena()
    print "phenomena extracted from file:"
    print file_phen
    print "Number of phenomena in the list: " + str(len(file_phen))
    for item in file_phen:


        print "Searching for phenomenon:"
        print item
        print " in database."


        query = create_query(item)
        print "Query ctreated : " + str(query)


        res = search_elasticsearch(query)

        print "Database returned :"
        print res


        #evaluating result.
        phen_id = is_valid_result(res)

        if phen_id is not None:
            print ""
            print "phenomenon: " + str(phen_id) + " exists in database."
            #Record the id and then index he file
        else:
            print "phenomenon needs to be inserted in the database."
            add_phenomenon(item)
            #insert phen in database and then insert file.

def main():
    #try:
    #    remove_index()
    #    print "REMOVED INDEX."
    #except:
    #    pass

    #print "CREATING INDEX."
    #create_index()
    #print "ADDING FILES."
    #file_ids = add_files()
    #print "CREATING PHENS."
    #phen_ids = add_phenomena()

    #print "UPDATING FILES."
    #update_files(file_ids, phen_ids)

    print "UPDATING PHENOMENA."
    file_phen = update_phenomena()

if __name__ == "__main__":

    main()
