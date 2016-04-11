import simplejson as json
import time
from elasticsearch import Elasticsearch
from elasticsearch import ElasticsearchException
from elasticsearch.exceptions import TransportError
from copy import deepcopy


def _get_host_string(config):
    """
    Take appropriate elements from a config dictionary and convert them into
    a string of format 'host:port'.
    :param dict config: Application configuration dictionary, including ES config.
    """
    host = config["es-configuration"]["es-host"]
    port = config["es-configuration"]["es-port"]
    return "%s:%d" % (host, port)


def create_index(config, elasticsearch):
    """
    Set up an index in ElasticSearch, given a configuration file path.
    :param dict config: Application configuration dictionary, including ES config.
    :param str index_settings_path: Path to index settings JSON document.
    """
    index_settings_path = config["es-configuration"]["es-index-settings"]
    index_name = config["es-configuration"]["es-index"]

    import simplejson as json  # Import here as unused in rest of module
    with open(index_settings_path, 'r') as settings:
        index_settings = json.load(settings)

    elasticsearch.indices.create(index=index_name, body=index_settings)

def search_database(es, index_l, type_l, query):

    """
    Executes a DSL query and returns the result.
    A delay is used because high rate of queries cause the 
    database to return an error.
    """

    res = es.search( index=index_l,
                     doc_type=type_l, 
                     body=query
                   )
    time.sleep(0.1)
    return res


def index_file(es, index_l, type_l, fid, fjson):

    """Indexes a document. """

    es.index(index=index_l, doc_type=type_l, id=fid, body=fjson, request_timeout=60)
    time.sleep(0.01)

bulk_requests = []
def index_phenomenon(es, index_l, type_l, phenomenon = None, threshold=0):

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

        #print "indexing "  + str(len(bulk_requests)) + " phenomena."

        for item in bulk_requests:
            json_query = json_query + item

        bulk_requests = []

        try:
            es.bulk(index=index_l, doc_type=type_l, body=json_query)
            #print json_query
            #print "Phenomenon saved in database."
            time.sleep(1) # Make sure that thi sis submitted before we query again the database.
        except Exception as ex:
            print "Error ocured during indexing of phenomenon."

    return pid

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
                es_subquery_name_template_copy["match_phrase"]["attributes.name"] = str(item[key])
                es_subquery_template_copy["nested"]["query"]["bool"]["must"].append(es_subquery_name_template_copy)
                name = False
                number_of_attributes = number_of_attributes +1
            else:
                es_subquery_value_template_copy = deepcopy(es_subquery_value_template)
                es_subquery_value_template_copy["match_phrase"]["attributes.value"] = str(item[key])
                es_subquery_template_copy["nested"]["query"]["bool"]["must"].append(es_subquery_value_template_copy)
                name = True

        es_query_template["query"]["bool"]["must"].append(es_subquery_template_copy)

        #es_subquery_count_copy = deepcopy(es_subquery_count)
    es_subquery_count["match"]["attribute_count"] = number_of_attributes
    es_query_template["query"]["bool"]["must"].append(es_subquery_count)

    return es_query_template

