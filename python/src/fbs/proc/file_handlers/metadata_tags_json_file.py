'''
Created on 17 May 2016

@author: kleanthis
'''
from fbs.proc.file_handlers.generic_file import GenericFile
import fbs.proc.common_util.util as util
import json
import logging

class MetadataTagsJsonFile(GenericFile):
    """
    Class for returning basic information about the content
    of an meta_data_tags.Json file.
    """

    def __init__(self, file_path, level, additional_param=None, **kwargs):
        GenericFile.__init__(self, file_path, level, **kwargs)
        self.FILE_FORMAT = "Metadata tags json"

    @staticmethod
    def get_phenomena(json_file):

        phenomena = json_file["phenomena"]

        phen_list = []
        for item in phenomena:
            new_phenomenon = {}
            attr_list = []
            for key in item:
                phen_attr = {}
                phen_attr["name"]  = key
                phen_attr["value"] = item[key]

                attr_list.append(phen_attr)

            new_phenomenon["attributes"] = attr_list

            phen_list.append(new_phenomenon)

        file_phenomena = util.build_phenomena(phen_list)
        return file_phenomena

    @staticmethod
    def get_temporal(json_content):
        start_time = util.date2iso(json_content["time"][0])
        end_time   = util.date2iso(json_content["time"][1])

        return {'time_range': {'gte': start_time, 'lte': end_time}}

    @staticmethod
    def get_geospatial(json_conent):
        try:
            # "geospatial": [-180.0, 90, 180, -90],
            lon_l = json_conent["geospatial"][0]
            lat_l = json_conent["geospatial"][1]
            lon_u = json_conent["geospatial"][2]
            lat_u = json_conent["geospatial"][3]

            return {'coordinates': {'type': 'envelope', 'coordinates': [[round(lon_l, 3), round(lat_l, 3)],
                                                                       [round(lon_u, 3), round(lat_u, 3)]]}}
        except Exception as e:
            logging.error(e)
            return None

    def get_metadata_level2(self):

        #Get basic file info.
        file_info = self.get_metadata_level1()

        if file_info is not None:

            self.handler_id = "Metadata tags json handler level 2."
            try:
                with open(self.file_path) as reader:
                    metadata = json.load(reader)

                file_info[0]["info"]["read_status"] = "Successful"
                phen_list = self.get_phenomena(metadata)

            except Exception:
                file_info[0]["info"]["read_status"] = "Read Error"
                phen_list = None

        else:
            return None

        return file_info + (phen_list, )

    def get_metadata_level3(self):
        #Get basic file info.
        file_info = self.get_metadata_level1()

        if file_info is not None:

            self.handler_id = "Metadata tags json handler level 3."

            try:
                with open(self.file_path) as reader:
                    metadata = json.load(reader)

            except Exception:
                # Error reading the file
                file_info[0]["info"]["read_status"] = "Read Error"
                return file_info

            phen_list = self.get_phenomena(metadata)

        else:
            return None

        spatial = self.get_geospatial(metadata)
        temporal = self.get_temporal(metadata)

        if temporal:
            file_info[0]["info"]["temporal"] = temporal

        file_info[0]["info"]["read_status"] = "Successful"

        return file_info + phen_list + (spatial, )

    def get_metadata(self):

        if self.level == "1":
            res = self.get_metadata_level1()
        elif self.level == "2":
            res = self.get_metadata_level2()
        elif self.level == "3":
            res = self.get_metadata_level3()

        res[0]["info"]["format"] = self.FILE_FORMAT

        return res

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


if __name__ == "__main__":
    import datetime
    import sys

    # run test
    try:
        level = str(sys.argv[1])
        file = sys.argv[2]
    except IndexError:
        level = '1'
        file = '/badc/ukcip02/data/50km_resolution/metadata_tags.json'

    mdf = MetadataTagsJsonFile(file,level)
    start = datetime.datetime.today()
    print( mdf.get_metadata())
    end = datetime.datetime.today()
    print( end-start)