'''
Created on 2 Jun 2016

@author: kleanthis
'''
from processing.file_handlers.generic_file import GenericFile
import processing.common_util.util as util
import csv
import re

class BadcCsvFile(GenericFile):

    def __init__(self, file_path, level, additional_param=None):
        GenericFile.__init__(self, file_path, level)
        self.handler_id = "Badc csv."
        self.FILE_FORMAT = "Badc csv."

    def get_handler_id(self):
        return self.handler_id

    def get_phenomena(self, fp):

        phen_list = []
        phenomenon =\
        {
         "id" : "",
         "attribute_count" : "",
         "attributes" :[]
        }
        phenomena = {}
        date = None
        location = None


        reader = csv.reader(fp)
        for row in reader:
            if row[0] == "data":
                break
            elif row[1] == "G":
                if row[0] == "date_valid":
                    date = row[2]
                if row[0] == "location":
                    location = row[2]
                continue
            else:
                if row[1] in phenomena: 
                    phenomena[row[1]]["attributes"] .append({"name": row[0], "value": re.sub(r'[^\x00-\x7F]+',' ', row[2])})
                    phenomena[row[1]]["attribute_count"] = phenomena[row[1]]["attribute_count"] + 1
                else:
                    new_phenomenon = phenomenon.copy()
                    new_phenomenon["attributes"] = []
                    new_phenomenon["attributes"].append({"name": row[0], "value": re.sub(r'[^\x00-\x7F]+',' ', row[2])})
                    new_phenomenon["attribute_count"] = 1
                    phenomena[row[1]] = new_phenomenon

        for key in phenomena.keys():
            phen_list.append(phenomena[key])

        return (phen_list, location, date)

    def get_metadata_badccsv_level2(self):
        self.handler_id = "Csv handler level 2."

        file_info = self.get_metadata_generic_level1()

        if file_info is not None:
            fp = open(self.file_path)
            phen = self.get_phenomena(fp)

            return  file_info +  (phen[0],)
        else:
            return None

    def get_metadata_badccsv_level3(self):
        self.handler_id = "Csv handler level 3."

        file_info = self.get_metadata_generic_level1()

        if file_info is not None:
            fp = open(self.file_path)
            phen = self.get_phenomena(fp)


            #l1  =  max(geospatial["lat"])
            #l2  =  min(geospatial["lat"])

            #lo1  = max(geospatial["lon"])
            #lo2 =  min(geospatial["lon"])


            #file_info[0]["info"]["spatial"] =  {"coordinates": {"type": "envelope", "coordinates": [[l1, lo1], [l2, lo2]] } }
            if phen[2] is not None:
                file_info[0]["info"]["temporal"] = {"start_time": phen[2], "end_time": phen[2] }

            return file_info +  (phen[0],)
        else:
            return None

    def get_metadata(self):

        if self.level == "1":
            res = self.get_metadata_generic_level1()
        elif self.level == "2":
            res = self.get_metadata_badccsv_level2()
        elif self.level == "3":
            res = self.get_metadata_badccsv_level3()

        res[0]["info"]["format"] = self.FILE_FORMAT

        return res

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass
