from proc.file_handlers.generic_file import GenericFile
import nappy
import proc.common_util.util as util


class NasaAmesFile(GenericFile):
    """
    Class for returning basic information about the content
    of an nasaames file.
    """

    def __init__(self, file_path, level, additional_param=None):
        GenericFile.__init__(self, file_path, level)
        self.FILE_FORMAT = "NASA Ames"

    def get_handler_id(self):
        return self.handler_id

    def phenomena(self):

        try:
            na_fhandle = nappy.openNAFile(self.file_path)

            variables = {}
            for var in na_fhandle.getVariables():
                if util.is_valid_phen_attr(var[1]):
                    variables.update({
                        var[0]: {
                            "name": var[0],
                            "units": var[1]
                        }
                    })

            variables = [util.Parameter(k, other_params=var) for (k, var) in variables.iteritems()]
            return variables
        except Exception:
            return None

    @util.simple_phenomena
    def get_phenomena(self):

        phen_list = []
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

        nasaames_phenomena = self.phenomena()

        if nasaames_phenomena is None:
            return None

        #List of phenomena
        for item in nasaames_phenomena:           #get all parameter objects.

            phen_attr_list = []
            #name = item.get_name()                #get phenomena name.
            #phen_attr["name"] = "var_id"
            #phen_attr["value"] = str(unicode(name).strip())

            phen_attr_list = item.get()
            #phen_attr_list.append(phen_attr)
            attr_count = len(phen_attr_list)

            new_phenomenon = phenomenon.copy()
            new_phenomenon["attributes"] = phen_attr_list
            new_phenomenon["attribute_count"] = attr_count

            phen_list.append(new_phenomenon)

        return phen_list


    def get_metadata_nasaames_level2(self):

        #Get basic file info.
        file_info = self.get_metadata_generic_level1()

        if file_info is not None:

            self.handler_id = "Nasaames handler level 2."

            phen_list = self.get_phenomena()
            if phen_list != None:
                file_info[0]["info"]["read_status"] = "Successful"
                return file_info +  phen_list
            else:
                # get_phenomena is None, error reading file.
                file_info[0]["info"]["read_status"] = "Read Error"
                return file_info

        else:
            return None

    def get_metadata_nasaames_level3(self):
        res = self.get_metadata_nasaames_level2()
        self.handler_id = "Nasaames handler level 3."
        return res

    def get_metadata(self):

        if self.level == "1":
            res = self.get_metadata_generic_level1()
        elif self.level == "2":
            res = self.get_metadata_nasaames_level2()
        elif self.level == "3":
            res = self.get_metadata_nasaames_level3()

        res[0]["info"]["format"] = self.FILE_FORMAT

        return res

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass
