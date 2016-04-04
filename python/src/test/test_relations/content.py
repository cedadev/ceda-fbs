import random, string

mapping = {
 "mappings" : 
 {
  "phenomenon" :
  {
   "properties" :
   {
    "id" :
    {
     "type" : "string"
    },
    "attribute_count": 
    {
     "type" : "short"
    },
    "attributes" :
    {
     "type" : "nested",
     "properties":
     {
      "name" : {"type": "string" },
      "value" : {"type": "string" }
     }
    }
   }
  },
  "file" : 
  {
   "properties" : 
   {
    "info" : 
    {
     "properties" : 
     {
      "directory" : 
      {
       "type" : "string", "index" : "not_analyzed" 
      },
      "format" : 
      {
       "type" : "string"
      },
      "md5" : 
      {
       "type" : "string"
      },
      "name" : 
      {
       "type" : "string", "index" : "not_analyzed" 
      },
      "name_auto" : 
      { 
       "type": "completion", "search_analyzer": "simple", "analyzer": "simple", "payloads": False
      },
      "phenomena" : 
      {
       "type" : "string"
      },
      "size" : 
      {
       "type" : "long"
      },
      "type" : 
      {
       "type" : "string"
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

#phenomena that needs to be inserted
#rainfall: {"long_name": "rainfall", "units": "mm"}
#temperature:  {"long_name": "temperature", "units": "K"}
#pressure:  {"long_name": "pressure", "units": "hPa"}
#pressure subset (subset of (c)):  {"long_name": "pressure"}


rainfall =\
{
  "id" : "2017",
  "attribute_count" : "2",
  "attributes" : 
  [ 
   { "name" : "long_name", "value" : "rainfall" },
   { "name" : "units", "value" : "mm"} 
  ]
}

temperature =\
{
  "id" : "2017",
  "attribute_count" : "2",
  "attributes" : 
  [ 
   { "name" : "long_name", "value" : "temperature"},
   { "name" : "units", "value" : "K"}
  ]
}

pressure =\
{
  "id" : "2017",
  "attribute_count" : "2",
  "attributes" : 
  [ 
    {"name" : "long_name", "value" : "pressure"},
    {"name" : "units", "value" : "hPa"}
  ]
}

pressure_sub =\
{
  "id" : "2017",
  "attribute_count" : "1",
  "attributes" : 
  [ 
   {"name" : "long_name", "value" : "pressure" }
  ]
}

phen = [rainfall, temperature, pressure, pressure_sub]

def randomword(length):
    return ''.join(random.choice(string.lowercase) for i in range(length))

def create_random_phenomenon():
    name = randomword(10)
    value = randomword(10)

    phen = {
            "id" : "1",
            "attribute_count" : "1",
            "attributes" : [ {"long_name" : name, "units" : value } ]
           }
    return phen

def get_file_phenomena():
    random_phen = create_random_phenomenon()
    print "Random phenomenon created :"
    print random_phen
    file_phenomena = phen[:]
    file_phenomena.append(random_phen)
    print file_phenomena
    return file_phenomena

def get_phenomenon(s):
    return phen[s]

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



