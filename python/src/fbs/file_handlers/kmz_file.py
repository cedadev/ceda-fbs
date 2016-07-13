'''
Created on 31 May 2016

@author: kleanthis
'''
from fbs.file_handlers.generic_file import GenericFile
import fbs_lib.util as util
import zipfile
import xmltodict
import os

#from fastkml import  kml
from pykml import parser
import datetime


class KmzFile(GenericFile):
    """
    Class for returning basic information about the content
    of an nasaames file.
    """

    def __init__(self, file_path, level, additional_param=None):
        GenericFile.__init__(self, file_path, level)
        self.handler_id = "kmz file handler."
        self.FILE_FORMAT = "KMZ"

    def get_handler_id(self):
        return self.handler_id

    def find_elements(self, node, item_list, result):
        for key, item in node.items():
            if isinstance(item, dict):
                self.find_elements(item, item_list, result)
            else:
                if isinstance(item, list):
                    for sub_item in item:
                        if isinstance(sub_item, dict):
                            self.find_elements(sub_item, item_list, result)
                        else:
                            #print key, " : ", item
                            if key in item_list:
                                result.append(item)
                else:
                    #print key, " : ", item
                    if key in item_list:
                        result.append(key)
                        result.append(item)

    def get_metadata_kmz_level3(self):
        self.handler_id = "MAnifest handler level 3."
        spatial = None

        res = self.get_metadata_generic_level1()
        # this is what i need to find.
        #len(parsed["kml"]["Document"]["Folder"][1]["Placemark"])

        if self.file_path.endswith('.kmz'):
            archive = zipfile.ZipFile(self.file_path, 'r')
            uncompressed_file = os.path.basename(self.file_path)
            uncompressed_file = uncompressed_file.replace(".kmz", ".kml")
            xml_doc = archive.read(uncompressed_file)
        else:
            with open(self.file_path, 'r') as xml_file:
                xml_doc = xml_file.read()

        xml_dict = xmltodict.parse(xml_doc)

        number_of_records = len(xml_dict["kml"]["Document"]["Folder"][1]["Placemark"])

        lat_l = []
        lon_l = []
        lat_u = []
        lon_u = []

        dates = [] 
        times = []
        for i in range(0, number_of_records):
            coordinates = xml_dict["kml"]["Document"]["Folder"][1]["Placemark"][i]["LineString"]["coordinates"]
            date = xml_dict["kml"]["Document"]["Folder"][1]["Placemark"][i]["description"]["table"]["tr"][1]["td"][1]
            time = xml_dict["kml"]["Document"]["Folder"][1]["Placemark"][i]["description"]["table"]["tr"][2]["td"][1]
            lon_l.append(coordinates.split(" ")[0].split(",")[0])
            lat_l.append(coordinates.split(" ")[0].split(",")[1])
            lon_u.append(coordinates.split(" ")[1].split(",")[0])
            lat_u.append(coordinates.split(" ")[1].split(",")[1])
            dates.append(date) 
            times.append(time)


        c1 = min(lat_l)
        c2 = min(lon_l)
        c3 = max(lat_u)
        c4 = max(lon_u)
        dt = datetime.datetime.strptime(min(dates), '%d-%m-%Y')
        start_date = "{}-{}-{}".format(dt.year, dt.month, dt.day)
        dt = datetime.datetime.strptime(max(dates), '%d-%m-%Y')
        end_date   = "{}-{}-{}".format(dt.year, dt.month, dt.day)

        spatial =  {'coordinates': {'type': 'envelope', 'coordinates': [[c2, c1 ], [c4, c3]] } }
        res[0]["info"]["temporal"] = {'start_time': start_date , 'end_time': end_date }#"1975-01-01"

        return res + (None, spatial, )

    def get_metadata(self):

        if self.level == "1":
            res = self.get_metadata_generic_level1()
        elif self.level == "2":
            res = self.get_metadata_generic_level1()
        elif self.level == "3":
            res = self.get_metadata_kmz_level3()

        res[0]["info"]["format"] = self.FILE_FORMAT

        return res

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass
