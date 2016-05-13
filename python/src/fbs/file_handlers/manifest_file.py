'''
Created on 12 May 2016

@author: kleanthis
'''

from fbs.file_handlers.generic_file import GenericFile
import fbs_lib.util as util
import xmltodict

class ManifestFile(GenericFile):
    """
    Class for returning basic information about the content
    of an nasaames file.
    """

    def __init__(self, file_path, level, additional_param=None):
        GenericFile.__init__(self, file_path, level)
        self.handler_id = "MAnifest handler level 3."
        self.FILE_FORMAT = "NASA Ames"

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

    def get_metadata_manifest_level3(self):
        self.handler_id = "MAnifest handler level 3."

        res = self.get_metadata_generic_level1()

        elements_to_find = [
                            "safe:startTime",
                            "safe:stopTime",
                            "gml:coordinates"
                           ]

        document_file = open(self.file_path, "r") # Open a file in read-only mode
        original_doc = document_file.read()       # read the file object
        document = xmltodict.parse(original_doc)  # Parse the read document string
        print document

        result = []
        self.find_elements(document, elements_to_find, result)

        #print "================"
        #for item in result:
        #    print item  
        #u'-56.301735,-37.494080 -54.830536,-31.223965 -58.449139,-28.045124 -60.058411,-34.861740

        c1 = (result[5]).split(" ")[0].split(",")[0]
        c2 = (result[5]).split(" ")[1].split(",")[1]
        c3 = (result[5]).split(" ")[3].split(",")[0]
        c4 = (result[5]).split(" ")[3].split(",")[1]

        res[0]["info"]["spatial"] =  {'coordinates': {'type': 'envelope', 'coordinates': [[c1, c2], [c3, c4]] } }
        res[0]["info"]["temporal"] = {'start_time': result[1], 'end_time': result[3] }

        return res

    def get_metadata(self):

        if self.level == "1":
            res = self.get_metadata_generic_level1()
        elif self.level == "2":
            res = self.get_metadata_generic_level1()
        elif self.level == "3":
            res = self.get_metadata_manifest_level3()

        res[0]["info"]["format"] = self.FILE_FORMAT

        return res

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass
