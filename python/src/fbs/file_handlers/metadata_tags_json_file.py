'''
Created on 17 May 2016

@author: kleanthis
'''
from fbs.file_handlers.generic_file import GenericFile
import fbs_lib.util as util
import json


class MetadataTagsJsonFile(GenericFile):
    """
    Class for returning basic information about the content
    of an meta_data_tags.Json file.
    """

    def __init__(self, file_path, level, additional_param=None):
        GenericFile.__init__(self, file_path, level)
        self.FILE_FORMAT = "Metadata tags json"

    def get_handler_id(self):
        return self.handler_id

    def get_phenomena(self, json_file):

        phenomena = json_file["phenomena"]

        phenomenon =\
        {
         "id" : "",
         "attribute_count" : "",
         "attributes" :[]
        }

        phen_attr =\
        {
         "name" : "",
         "value": ""
        }

        phenomena_list = []
        for item in phenomena:
            phen_attr["name"]  = "var_id"
            phen_attr["value"] = item["var_id"]

            attr_list = []
            attr_list.append(phen_attr.copy())

            new_phenomenon = phenomenon.copy()
            new_phenomenon["attributes"] = attr_list
            new_phenomenon["attribute_count"] = 1

            phenomena_list.append(new_phenomenon)

        return phenomena_list


    def get_metadata_tags_json_level2(self):

        #Get basic file info.
        file_info = self.get_metadata_generic_level1()

        if file_info is not None:

            self.handler_id = "Metadata tags json handler level 2."
            metadata = json.loads(open(self.file_path).read())

            phen_list = self.get_phenomena(metadata)

        else:
            return None

        return file_info + (phen_list, )

    def get_metadata_tags_json_level3(self):
        #Get basic file info.
        file_info = self.get_metadata_generic_level1()

        if file_info is not None:

            self.handler_id = "Metadata tags json handler level 2."
            metadata = json.loads(open(self.file_path).read())

            phen_list = self.get_phenomena(metadata)

        else:
            return None

        #"geospatial": [-180.0, 90, 180, -90],
        l1  = metadata["geospatial"][0]
        lo1 = metadata["geospatial"][1]
        l2  = metadata["geospatial"][2]
        lo2 = metadata["geospatial"][3]

        file_info[0]["info"]["spatial"] =  {'coordinates': {'type': 'envelope', 'coordinates': [[l1, lo1], [l2, lo2]]}}

        # "time": ["1859-01-01T00:00:00", "2016-03-04T23:59:59"]
        start_time = metadata["time"][0]
        end_time   = metadata["time"][1]
        file_info[0]["info"]["temporal"] = {'start_time': start_time, 'end_time': end_time }

        return file_info + (phen_list, )

    def get_metadata(self):

        if self.level == "1":
            res = self.get_metadata_generic_level1()
        elif self.level == "2":
            res = self.get_metadata_tags_json_level2()
        elif self.level == "3":
            res = self.get_metadata_tags_json_level3()

        res[0]["info"]["format"] = self.FILE_FORMAT

        return res

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass
