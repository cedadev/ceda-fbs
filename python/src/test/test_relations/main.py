from datetime import datetime
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
        time.sleep(0.3)

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
            print tmp
            doc["info"]["phenomena"] = tmp
        elif count >= 500 and count <= 599:
            tmp = phen_ids[0:3]
            print tmp
            doc["info"]["phenomena"] = tmp
        elif count >= 600 and count <= 699:
            tmp = [phen_ids[0:2], phen_ids[3]]
            print tmp
            doc["info"]["phenomena"] = tmp

        body_l["doc"] = doc
        es.update(index=INDEX, doc_type="file", id=item, body=body_l)
        count = count + 1
        print count

    def add_new_phenomena():
        file_phen = get_file_phenomena()
        #check if phens exist in database
        #if not add them
        #at the end post also the file

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
