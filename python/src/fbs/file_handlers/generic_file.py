import logging
import os
import ntpath
import json
from es import ops

class  GenericFile(object):
    """
    Class for returning basic information about a file.
    """

    def __init__(self, file_path, level, additional_param=None):
        self.file_path = file_path
        self.level = level
        self.handler_id = None


    def get_handler_id(self):
        return self.handler_id

    def get_properties_generic_level1(self):
        """
        Scans the given file and returns information about 
        the file not the content.
        :returns: A dict containing summary information.
        """

        self.handler_id = "Generic level 1."

        #Do the basic checking, if file exists 
        #and that it is not a symbolic link.
        if ( self.file_path is None
             or not os.path.isfile(self.file_path)
             or os.path.islink(self.file_path)
           ):
            return None

        file_info = {}
        info = {}

        #Basic information. 
        info["name"] = os.path.basename(self.file_path) #ntpath.basename(file_path)
        info["name_auto"] = info["name"]
        info["directory"] = os.path.dirname(self.file_path)

        info["size"] = round(os.path.getsize(self.file_path) / (1024*1024.0), 3)

        info["type"] = "file"

        info["md5"] = ""

        file_info["info"] = info

        return file_info

    def get_properties_generic_level2(self):

        """
         Wrapper for method get_properties_generic_level1().
        :returns: A dict containing information compatible with current es ops.
        """

        file_info = self.get_properties_generic_level1()

        self.handler_id = "Generic level 2."

        if file_info is None:
            return None

        return file_info

    def get_properties_generic_level3(self):
        file_info = self.get_properties_generic_level2()
        self.handler_id = "Generic level 3."
        return file_info

    def get_properties(self):

        if self.level == "1":
            return self.get_properties_generic_level1()
        elif self.level == "2":
            return self.get_properties_generic_level2()
        elif self.level == "3":
            return self.get_properties_generic_level3()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass
