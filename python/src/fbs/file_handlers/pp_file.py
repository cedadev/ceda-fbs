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

    def getVariableMetadata(self, var_id, handle):
        """
        Gets variable metadata object from a data file.
        """
        try:
            var = handle[var_id]
        except:
            raise Exception("Cannot find variable '%s' in file '%s'." % (var_id, self.handle.id))
        return var


    def getBoundingBox(self, var_id, handle):
        """
        Returns the horizontal domain as (west, south, east, north).
        """
        var = self.getVariableMetadata(var_id, handle)

        try: 
            lat = var.getLatitude()[:]
            lon = var.getLongitude()[:]
        except:
            raise Exception("Could not identify latitude and longitude axes for '%s'" % var_id)

        if lat[-1] < lat[0]: 
            lat = list(lat) 
            lat.reverse()

        (south, north) = (lat[0], lat[-1])
        (west, east) = (lon[0], lon[-1])

        bbox = (west, south, east, north)
        return bbox

    def getTemporalDomain(self, var_id, handle):
        """
        Returns the temporal domain as a tuple of (start time, end time,
        (interval value, interval units)).
        """
        var = self.getVariableMetadata(var_id, handle)
        time_axis = var.getTime()

        # Get component time only once in case large
        axis_comp_times = time_axis.asComponentTime()
        start_time = str(axis_comp_times[0]).replace(" ", "T")
        end_time = str(axis_comp_times[-1]).replace(" ", "T")

        time_units = time_axis.units.split()[0]

        if time_units[-1] == "s":  time_units = time_units[:-1]

        return (start_time, end_time, time_units)

    def get_metadata_pp_level2(self):
        """
        Wrapper for method phenomena().
        :returns:  A dict containing information compatible with current es index level 2.
        """
        phenomenon =\
        {
         "id" : "",
         "attribute_count" : "",
         "attributes" :[]
        }

        #Get basic file info.
        file_info = self.get_metadata_generic_level1()

        if file_info is not None:
            try:
                self.handler_id = "pp handler level 2."

                pp_file_content=cdms.open(self.file_path)
                var_ids = pp_file_content.listvariables()

                #Filter long values and overwrite duplicates.
                phen_list = []
                for var_id in var_ids:
                    metadata_dict = pp_file_content[var_id].attributes
                    phen_attr_list = []
                    attr_count = 0
                    for key in metadata_dict.keys():

                        value = str(metadata_dict[key])
                        if     len(key) < util.MAX_PAR_LENGTH \
                           and len(value) < util.MAX_PAR_LENGTH:
                            phen_attr =\
                            {
                              "name" : str(key.strip()),
                              "value": str(unicode(value).strip())
                            }

                        phen_attr_list.append(phen_attr.copy())
                        attr_count = attr_count + 1

                    #Dict of phenomenon attributes.
                    if len(phen_attr_list) > 0:
                        new_phenomenon = phenomenon.copy() 
                        new_phenomenon["attributes"] = phen_attr_list
                        new_phenomenon["attribute_count"] = attr_count

                        phen_list.append(new_phenomenon)

                pp_file_content.close()

                return file_info +  (phen_list, )
            except Exception as ex:
                return file_info
        else:
            return None

    def normalize_coord(self, coord):
        if coord > 180:
            coord = coord - 360
        return coord

    def get_metadata_pp_level3(self):
        """
        Wrapper for method phenomena().
        :returns:  A dict containing information compatible with current es index level 2.
        """

        #Get basic file info.
        file_info = self.get_metadata_generic_level1()

        if file_info is not None:
            try:
                self.handler_id = "pp handler level 2."
                phenomena_list = []
                phenomenon_parameters_dict = {}
                list_of_phenomenon_parameters = []
                phenomenon_attr = {}
                lat_l = []
                lat_u = []
                lon_l = []
                lon_u = []
                start_time = []
                end_time = []

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
                    try :
                        spatial  = self.getBoundingBox(var_id, pp_file_content)
                        temporal = self.getTemporalDomain(var_id, pp_file_content)

                        #geospatial data.
                        lat_l.append(spatial[0])
                        lon_l.append(spatial[1])
                        lat_u.append(spatial[2])
                        lon_u.append(spatial[3])

                        #temporal
                        start_time.append(temporal[0])
                        end_time.append(temporal[1])

                    except Exception as ex:
                        continue

                min_lon_l = self.normalize_coord(min(lat_l))
                min_lat_l = self.normalize_coord(min(lon_l))
                max_lon_u = self.normalize_coord(max(lat_u))
                max_lat_u = self.normalize_coord(max(lon_u))


                file_info["spatial"] =  {'coordinates': {'type': 'envelope', 'coordinates': [[min_lat_l, min_lon_l], [max_lat_u, max_lon_u]]}}
                file_info["temporal"] = {'start_time': min(start_time), 'end_time': max(end_time) }

                pp_file_content.close()
                file_info["phenomena"] = phenomena_list
                return file_info
            except Exception as ex:
                return file_info
        else:
            return None

    def get_metadata(self):

        if self.level == "1":
            res = self.get_metadata_generic_level1()
        elif self.level == "2":
            res = self.get_metadata_pp_level2()
        elif self.level == "3":
            res = self.get_metadata_pp_level3()

        res[0]["info"]["format"] = self.FILE_FORMAT

        return res

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass
