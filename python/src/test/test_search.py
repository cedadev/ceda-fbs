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
HOST2 = "jasmin-es1.ceda.ac.uk"
#HOST = HOST2
PORT = "9200"
INDEX = "archive_level_2_1"

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

    res = es.search( index=INDEX,
                     doc_type="file", 
                     body=query\
                     #request_timeout=60,
                     #size = 5000\
                   )

    """
    This is a typical result:
    {u'hits': 
    {u'hits': 
    [
     {u'_score': 1.0, u'_type': u'file', u'_id': u'e04a1b5ca0612ddd0920f129627bad15e0482bef', u'_source': 
     {u'info': {u'name': u'ORCA1-R07_y1963m01_VT.nc', u'format': u'NetCDF', 
     u'phenomena': [6678973056177272973, 9164619183587984448, 3321250461259629633, 
                    7666618002070728122, 8030759105886828108, 8624922703478122904, 
                    8675747911943070266, 980326250668624946], 
     u'name_auto': u'ORCA1-R07_y1963m01_VT.nc', 
     u'directory': u'/badc/rapid/data/Haines_Rapid1_Round2/ORCA1/ORCA1-R07-MEAN/RA_t3/1963', 
     u'md5': u'', u'type': u'file', u'size': 75.003}}, 
     u'_index': u'archive_level_2_1'}, 
     {u'_score': 1.0, u'_type': u'file', u'_id': u'9e7bf284c2d9428947bc0cf46bd9cea5d77df2be', u'_source': 
     {u'info': {u'name': u'ORCA1-R07_y1963m05_icemod2.nc', u'format': u'NetCDF', u'phenomena': [6678973056177272973, 9164619183587984448, 3321250461259629633, 1901573199066746681], u'name_auto': u'ORCA1-R07_y1963m05_icemod2.nc', u'directory': u'/badc/rapid/data/Haines_Rapid1_Round2/ORCA1/ORCA1-R07-MEAN/RA_t3/1963', u'md5': u'', u'type': u'file', u'size': 0.807}}, u'_index': u'archive_level_2_1'}, {u'_score': 1.0, u'_type': u'file', u'_id': u'7c8c1a79f49f10850e9c1e4100014e8896363d3b', u'_source': {u'info': {u'name': u'ORCA1-R07_y1963m06_gridU2.nc', u'format': u'NetCDF', u'phenomena': [6678973056177272973, 9164619183587984448, 1297253927527245694, 7666618002070728122, 7938487883748190754], u'name_auto': u'ORCA1-R07_y1963m06_gridU2.nc', u'directory': u'/badc/rapid/data/Haines_Rapid1_Round2/ORCA1/ORCA1-R07-MEAN/RA_t3/1963', u'md5': u'', u'type': u'file', u'size': 19.356}}, u'_index': u'archive_level_2_1'}, {u'_score': 1.0, u'_type': u'file', u'_id': u'd418c008ca1c68fc6b7ac8e2b53e47e8d9aedec7', u'_source': {u'info': {u'name': u'ORCA1-R07_y1963m06_gridV2.nc', u'format': u'NetCDF', u'phenomena': [6678973056177272973, 9164619183587984448, 7384095765192131957, 7666618002070728122, 124604983952914977], u'name_auto': u'ORCA1-R07_y1963m06_gridV2.nc', u'directory': u'/badc/rapid/data/Haines_Rapid1_Round2/ORCA1/ORCA1-R07-MEAN/RA_t3/1963', u'md5': u'', u'type': u'file', u'size': 19.356}}, u'_index': u'archive_level_2_1'}, {u'_score': 1.0, u'_type': u'file', u'_id': u'21e01306db6026540973b0474a7e1350ef58343e', u'_source': {u'info': {u'name': u'ORCA1-R07_y1963m07_VT.nc', u'format': u'NetCDF', u'phenomena': [6678973056177272973, 9164619183587984448, 3321250461259629633, 7666618002070728122, 8030759105886828108, 8624922703478122904, 8675747911943070266, 980326250668624946], u'name_auto': u'ORCA1-R07_y1963m07_VT.nc', u'directory': u'/badc/rapid/data/Haines_Rapid1_Round2/ORCA1/ORCA1-R07-MEAN/RA_t3/1963', u'md5': u'', u'type': u'file', u'size': 75.003}}, u'_index': u'archive_level_2_1'}, {u'_score': 1.0, u'_type': u'file', u'_id': u'34fed1daaab4e682b4b200475ad05a25ec2c600f', u'_source': {u'info': {u'name': u'ORCA1-R07_y1963_SUMM_gridT2.nc', u'format': u'NetCDF', u'phenomena': [6678973056177272973, 9164619183587984448, 3321250461259629633, 7666618002070728122, 3786401385713489802], u'name_auto': u'ORCA1-R07_y1963_SUMM_gridT2.nc', u'directory': u'/badc/rapid/data/Haines_Rapid1_Round2/ORCA1/ORCA1-R07-MEAN/RA_t3/1963', u'md5': u'', u'type': u'file', u'size': 1.211}}, u'_index': u'archive_level_2_1'}, {u'_score': 1.0, u'_type': u'file', u'_id': u'cf82a5d4e36502c292ede1e9a90c95271c1a6491', u'_source': {u'info': {u'name': u'ORCA1-R07_y1963_SUMM_syn2.nc', u'format': u'NetCDF', u'phenomena': [6678973056177272973, 9164619183587984448, 3321250461259629633, 7666618002070728122], u'name_auto': u'ORCA1-R07_y1963_SUMM_syn2.nc', u'directory': u'/badc/rapid/data/Haines_Rapid1_Round2/ORCA1/ORCA1-R07-MEAN/RA_t3/1963', u'md5': u'', u'type': u'file', u'size': 0.808}}, u'_index': u'archive_level_2_1'}, {u'_score': 1.0, u'_type': u'file', u'_id': u'8dc4497afc0cfe4cf9bc59a4413d0e065470f8bd', u'_source': {u'info': {u'name': u'ORCA1-R07_y1963_WINT_gridU.nc', u'format': u'NetCDF', u'phenomena': [6678973056177272973, 9164619183587984448, 1297253927527245694, 7666618002070728122, 429516276193964193, 4327381651336915226, 8616516905742624115], u'name_auto': u'ORCA1-R07_y1963_WINT_gridU.nc', u'directory': u'/badc/rapid/data/Haines_Rapid1_Round2/ORCA1/ORCA1-R07-MEAN/RA_t3/1963', u'md5': u'', u'type': u'file', u'size': 38.309}}, u'_index': u'archive_level_2_1'}, {u'_score': 1.0, u'_type': u'file', u'_id': u'0e0123a397b17cd94738690cbd3c742afff671ca', u'_source': {u'info': {u'name': u'ORCA1-R07_y1963_WINT_gridT.nc', u'format': u'NetCDF', u'phenomena': [6678973056177272973, 9164619183587984448, 3321250461259629633, 7666618002070728122, 6766281168671471656, 7077142044740623473, 1070718445003165292, 3800052301331927580, 4772498151173528632, 3673588523775663871, 4976960849100353349, 8236491927475408048, 4834225652167002399, 404323094642897082, 916640353100055810, 8310860397563481947, 5753634427128719690], u'name_auto': u'ORCA1-R07_y1963_WINT_gridT.nc', u'directory': u'/badc/rapid/data/Haines_Rapid1_Round2/ORCA1/ORCA1-R07-MEAN/RA_t3/1963', u'md5': u'', u'type': u'file', u'size': 42.344}}, u'_index': u'archive_level_2_1'}, {u'_score': 1.0, u'_type': u'file', u'_id': u'827463c31b0cd210d0e93108f889883dcc9b0f6f', u'_source': {u'info': {u'name': u'ORCA1-R07_y1963_WINT_gridV.nc', u'format': u'NetCDF', u'phenomena': [6678973056177272973, 9164619183587984448, 7384095765192131957, 7666618002070728122, 489003067196031614, 991880722429564001, 3716865445702490112], u'name_auto': u'ORCA1-R07_y1963_WINT_gridV.nc', u'directory': u'/badc/rapid/data/Haines_Rapid1_Round2/ORCA1/ORCA1-R07-MEAN/RA_t3/1963', u'md5': u'', u'type': u'file', u'size': 38.309}}, u'_index': u'archive_level_2_1'}], u'total': 203, u'max_score': 1.0}, u'_shards': {u'successful': 8, u'failed': 0, u'total': 8}, u'took': 29, u'timed_out': False}

    """


    hits = res[u'hits'][u'hits']
    phenomena = []

    for item in hits:
        file_phenomena = item["_source"]["info"]["phenomena"]
        phenomena += file_phenomena

    phenomena_unique = []

    for item in phenomena:
        if item not in phenomena_unique:
            phenomena_unique.append(item)

    print phenomena_unique

    for item in phenomena_unique:
        res = es.get(index=INDEX, doc_type='phenomenon', id=item)
        print res



    #get phenomena
    """
    query_phen =\
    {
     "docs" : []
    }

    for item in phenomena_unique:
        phen =\
        {
         "_index" : INDEX,
         "_type" : "phenomenon",
         "_id" : item
        }
        json_query = json.dumps(phen) + "\n"

        query_phen["docs"].append(json_query)

    print query_phen

    json_query_phen = json.dumps(query_phen)

    print json_query_phen

    res = es.bulk(index=INDEX, doc_type="phenomenon", body=json_query_phen)

    print res
    """
    time.sleep(0.1)

    return phenomena_unique

def main():
    directory = raw_input("Please enter a directory to search for phenomena:")

    start = datetime.datetime.now()
    print "Script started at: %s" %(str(start))

    res = search_database_phenomena(directory)
    #print res

    end = datetime.datetime.now()
    print "Script ended at : %s it ran for : %s" %(str(end), str(end - start))

if __name__ == "__main__":

    main()
