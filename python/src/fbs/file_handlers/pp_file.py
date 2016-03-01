import cdms2 as cdms
import os
import fbs_lib.util as util


from fbs.file_handlers.generic_file import GenericFile

class PpFile(GenericFile):
    """
    Simple class for returning basic information about the content
    of an PP file.
    """

    def __init__(self, file_path, level, additional_param=None):
        GenericFile.__init__(self, file_path, level)
        self.FILE_FORMAT = "PP"
        self.handler_id = ""

    def get_handler_id(self):
        return self.handler_id

    def get_properties_pp_level2(self):
        """
        Wrapper for method phenomena().
        :returns:  A dict containing information compatible with current es index level 2.
        """

        #Get basic file info.
        file_info = self.get_properties_generic_level1()

        if file_info is not None:
            try:
                self.handler_id = "pp handler level 2."
                phenomena_list = []
                phenomenon_parameters_dict = {}
                list_of_phenomenon_parameters = []
                phenomenon_attr = {}

                pp_file_content=cdms.open(self.file_path)
                var_ids = pp_file_content.listvariables()

                #Filter long values and overwrite duplicates.
                for var_id in var_ids:
                    metadata_dict = pp_file_content[var_id].attributes
                    list_of_phenomenon_parameters = []
                    for key in metadata_dict.keys():
                        value = str(metadata_dict[key])

                        if     len(key) < util.MAX_PAR_LENGTH \
                           and len(value) < util.MAX_PAR_LENGTH:
                            phenomenon_attr["name"] = key
                            phenomenon_attr["value"] = value
                            list_of_phenomenon_parameters.append(phenomenon_attr.copy())

                    #Dict of phenomenon attributes.
                    phenomenon_parameters_dict["phenomenon_parameters"] = list_of_phenomenon_parameters
                    phenomena_list.append(phenomenon_parameters_dict.copy())

                pp_file_content.close()
                file_info["phenomena"] = phenomena_list
                return file_info
            except Exception as ex:
                return file_info
        else:
            return None

    def get_properties_pp_level3(self):
        return None

    def get_properties(self):

        if self.level == "1":
            res = self.get_properties_generic_level1()
        elif self.level == "2":
            res = self.get_properties_pp_level2()
        elif self.level == "3":
            res = self.get_properties_pp_level3()

        res["info"]["format"] = self.FILE_FORMAT

        return res

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass
