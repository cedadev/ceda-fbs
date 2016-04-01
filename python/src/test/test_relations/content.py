
mapping = {
  "mappings": {
 "phenomenon":
 {
 "properties":
 {
  "id":
  {
    "type": "string"
  },
  "parameters":
  {
   "type": "nested",
   "properties":
   {
    "name": {"type": "string" },
    "value": {"type": "string" }
   }
  }
 }
},
      "file": {
        "properties": {
          "info": {
            "properties": {
              "directory": {
               "type": "string", "index": "not_analyzed"
              },
              "format": {
                "type": "string"
              },
              "md5": {
                "type": "string"
              },
              "name": {
                "type": "string", "index": "not_analyzed"
              },
              "name_auto": {
                "type": "completion", "search_analyzer": "simple", "analyzer": "simple", "payloads": False
              },
              "phenomena": {
                "type": "string"
              },
              "size": {
                "type": "long"
              },
              "type": {
                "type": "string"
              }
            }
          }
        }
      }
    }
   }

test_file_dict = {
"info":
 {
  "directory": "testdir",
  "format": "file",
  "md5": "234",
  "name": "test_file1",
  "name_auto": "test_file1",
  "size": 100,
  "type": "NCDF",
  "phenomena": []
 }
}

def get_file_doc(name):
    d = test_file_dict.copy()
    d["info"]["name"] = name
    d["info"]["name_auto"] = name
    return d

OLD_test_phenomenon = {"parameters":
    [
    {"name": "phenomenon attr1", "value": "phenomenon value1"},
    {"name": "phenomenon attr2", "value": "phenomenon value2"},
    {"name": "phenomenon attr3", "value": "phenomenon value3"},
    {"name": "phenomenon attr4", "value": "phenomenon value4"}
  ]}

def get_phenomenon(s):
    d = {"parameters": []}
    for i in range(1, 4):
        d["parameters"].append({"name": "name %d - %s" % (i, s), "value": "value %d - %s" % (i, s)})

    return d 

"""
curl -XPOST "mist.badc.rl.ac.uk:9200/archive_level2/phenomenon/?pretty=true" -d' {
  "id": "2017",


}
"""


# query that can be used for searching for specific phenomenon:
query_phen = {
 "query":
 {
  "nested":
  {
   "path": "parameters",
   "query":
   {
    "bool":
    {
     "must":
     [
      { "match_phrase": { "parameters.name": "phenomenon attr100" } },
      { "match_phrase": { "parameters.value": "phenomenon value100" } }
     ]
    }
   }
  }
 },
    "size": 10
}

# query for searching for documents with specific phenomenon id:

# http://mist.badc.rl.ac.uk:9200/archive_level2/file/_search?q=info.phenomena:2016&pretty=true;



