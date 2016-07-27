import random, string
from copy import deepcopy

#mappings of index.
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

    """
    Returns a valid jason document for the file type.
    """

    d = test_file_dict.copy()
    d["info"]["name"] = name
    d["info"]["name_auto"] = name
    return d


#phenomena contained in the files.
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

#Phenomena that will be contained in every file.
phen = [rainfall, temperature, pressure, pressure_sub]

def randomword(length):
    return ''.join(random.choice(string.lowercase) for i in range(length))

def create_random_phenomenon():

    """
    Returns a phenomenon with random name and value.
    """

    name = randomword(10)
    value = randomword(10)

    phen = {
            "id" : "1",
            "attribute_count" : "1",
            "attributes" : [ {"name" : name, "value" : value } ]
           }

    return phen.copy()

def get_file_phenomena():

    """
    Returns phenomena contained in a file.
    The function returns one unique phenomeon for every file.
    """

    random_phen = create_random_phenomenon()
    #print "Random phenomenon created :"
    #print random_phen
    file_phenomena = phen[:]
    file_phenomena_real = deepcopy(file_phenomena)
    file_phenomena_real.append(random_phen)
    #print file_phenomena
    return file_phenomena_real

def get_file_phenomena_i(index):

    """
    Return file phenomena depending on the value of index.
    """

    if index <= 99:
        return [phen[0]]
    elif index >= 100 and index <= 199:
        return [phen[1]]
    elif index >= 200 and index <= 299:
        return [phen[2]]
    elif index >= 300 and index <= 399:
        return [phen[3]]
    elif index >= 400 and index <= 499:
        return phen[0:2]
    elif index >= 500 and index <= 599:
        return phen[0:3]
    elif index >= 600 and index <= 699:
        tmp_l = phen[0:2]
        tmp_l.append(phen[3])
        return tmp_l

def get_phenomenon(s):
    return phen[s]



