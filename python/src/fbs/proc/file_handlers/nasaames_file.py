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

    def get_phenomena(self):

        phen_list = []

        #List of phenomena
        nasaames_phenomena = self.phenomena()

        if nasaames_phenomena is None:
            return None

        # get all parameter objects.
        for item in nasaames_phenomena:

            phen_attr_list = item.get()

            new_phenomenon = {}
            new_phenomenon["attributes"] = phen_attr_list

            phen_list.append(new_phenomenon)

        file_phenomena = util.build_phenomena(phen_list)

        return file_phenomena


    def get_metadata_level2(self):

        #Get basic file info.
        file_info = self.get_metadata_level1()

        if file_info is not None:

            self.handler_id = "Nasaames handler level 2."

            phen_list = self.get_phenomena()
            try:
                if phen_list != None:
                    file_info[0]["info"]["read_status"] = "Successful"
                    return file_info +  phen_list
                else:
                    # get_phenomena returned None, error reading file
                    # Todo Change this so that errors propagate back up and are caught, not hidden with a None response

                    file_info[0]["info"]["read_status"] = "Read Error"
                    return file_info + (None,)
            except Exception:
                file_info[0]["info"]["read_status"] = "Read Error"
                return file_info + (None,)

        else:
            return None

    def get_metadata_level3(self):
        res = self.get_metadata_level2()
        self.handler_id = "Nasaames handler level 3."
        return res

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

    file = '/neodc/arsf/2008/VOC_05/VOC_05-2008_307_Chile/aimms/arsf-aimms20_arsf-dornier_20081102_r0_307_voc-05.na'
    naf = NasaAmesFile(file,level)
    start = datetime.datetime.today()
    print naf.get_metadata()
    end = datetime.datetime.today()
    print end-start
