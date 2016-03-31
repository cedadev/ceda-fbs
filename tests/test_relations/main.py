from datetime import datetime
import sys
sys.path.append(".")
import socket, time

from elasticsearch import Elasticsearch

from content import *

HOST = "mist.badc.rl.ac.uk"
PORT = "9200"
INDEX = "agrel"
es = Elasticsearch(hosts=[{"host": HOST, "port": PORT}])

def remove_index():
    res = es.indices.delete(index=INDEX)

def create_index():
    res = es.indices.create(index=INDEX, body=mapping)

def get_hash(fname):
    return abs(hash((socket.gethostname() + str(time.time()))))

def add_files():
    ids = []
    for i in range(1, 11):
        fname = "file_%02d" % i
        doc = get_file_doc(fname)
        id = get_hash(fname)
        es.index(index=INDEX, doc_type="file", id=id, body=doc)
        ids.append(id)

    return ids

def add_phenomena():
    ids = []

    for i in range(3):
        phen = test_phenomenon
        id = hash(str(phen))
        es.index(index=INDEX, doc_type="phenomenon", id=id, body=phen)
        ids.append(id)

    return ids

def update_files(file_ids, phen_ids):
    #doc = {"info": {"phenomena": phen_ids}}
    body_l= {}
    for item in file_ids:
        fname = "file_%02d" % 1
        doc = get_file_doc(fname)
        doc["info"]["phenomena"] = phen_ids
        body_l["doc"] = doc
        #doc = "{\"info\": {\"name_auto\": \"file_01\", \"name\": \"file_01\", \"format\": \"file\", \"directory\": \"testdir\", \"phenomena\": [\"8801316596223403580\", \"8801316596223403580\", \"8801316596223403580\"], \"size\": 100, \"type\": \"NCDF\", \"md5\": 234}}"
        #print body_l
        es.update(index=INDEX, doc_type="file", id=item, body=body_l)

def main():
    try:
        remove_index()
        print "REMOVED INDEX"
    except:
        pass

    print "CREATING INDEX"
    create_index()
    print "ADDING FILES"
    file_ids = add_files()
    print "CREATING PHENS"
    phen_ids = add_phenomena()

    print "UPDATING"
    update_files(file_ids, phen_ids)

if __name__ == "__main__":

    main()