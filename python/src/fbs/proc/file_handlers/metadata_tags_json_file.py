'''
Created on 17 May 2016

@author: kleanthis
'''
from proc.file_handlers.generic_file import GenericFile
import proc.common_util.util as util
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

    # @util.simple_phenomena
    def get_phenomena(self, json_file):

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


    def get_metadata_level2(self):

        #Get basic file info.
        file_info = self.get_metadata_level1()

        if file_info is not None:

            self.handler_id = "Metadata tags json handler level 2."
            try:
                metadata = json.loads(open(self.file_path).read())
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
        spatial = None
        phen_list = None

        if file_info is not None:

            self.handler_id = "Metadata tags json handler level 3."

            try:
                metadata = json.loads(open(self.file_path).read())

            except Exception:
                # Error reading the file
                file_info[0]["info"]["read_status"] = "Read Error"
                return file_info

            phen_list = self.get_phenomena(metadata)

        else:
            return None

        #"geospatial": [-180.0, 90, 180, -90],
        lon_l  = metadata["geospatial"][0]
        lat_l = metadata["geospatial"][1]
        lon_u  = metadata["geospatial"][2]
        lat_u = metadata["geospatial"][3]

        spatial =  {'coordinates': {'type': 'envelope', 'coordinates': [[round(lon_l, 3), round(lat_l, 3)], [round(lon_u, 3), round(lat_u, 3)]]}}

        # "time": ["1859-01-01T00:00:00", "2016-03-04T23:59:59"]
        start_time = util.date2iso(metadata["time"][0])
        end_time   = util.date2iso(metadata["time"][1])
        file_info[0]["info"]["temporal"] = {'start_time': start_time, 'end_time': end_time }

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
    except IndexError:
        level = '1'

    file = '/badc/ukcip02/data/50km_resolution/metadata_tags.json'
    mdf = MetadataTagsJsonFile(file,level)
    start = datetime.datetime.today()
    print mdf.get_metadata()
    end = datetime.datetime.today()
    print end-start