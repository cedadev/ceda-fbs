import gribapi as gapi
import fbs_lib.util as util

from fbs.file_handlers.generic_file import GenericFile

class GribFile(GenericFile):
    """
    Class for returning basic information about the content
    of an Grib file.
    """

    def __init__(self, file_path, level, additional_param=None):
        GenericFile.__init__(self, file_path, level)
        self.FILE_FORMAT = "GRIB"

    def get_handler_id(self):
        return self.handler_id

    def get_phenomena(self):

        phen_list = []
        phenomenon =\
        {
         "id" : "",
         "attribute_count" : "",
         "attributes" :[]
        }

        phen_keys = [
                      "paramId",
                      "cfNameECMF",
                      "cfName",
                      "cfVarName",
                      "units",
                      "nameECMF",
                      "name"
                    ]

        phen_attr =\
        {
         "name" : "",
         "value": ""
        }

        try:
            fd = open(self.file_path)

            while 1:
                gid = gapi.grib_new_from_file(fd)
                if gid is None: break

                phen_attr_list = []
                attr_count = 0
                for key in phen_keys:

                    if not gapi.grib_is_defined(gid, key):
                        continue

                    value = str(gapi.grib_get(gid, key))
                    if len(key) < util.MAX_ATTR_LENGTH \
                       and len(value) < util.MAX_ATTR_LENGTH \
                       and util.is_valid_phen_attr(value):

                        phen_attr["name"] = str(key.strip())
                        phen_attr["value"] = str(unicode(value).strip())

                        if phen_attr not in phen_attr_list:
                            phen_attr_list.append(phen_attr.copy())
                            attr_count = attr_count + 1

                if len(phen_attr_list) > 0:
                    new_phenomenon = phenomenon.copy() 
                    new_phenomenon["attributes"] = phen_attr_list
                    new_phenomenon["attribute_count"] = attr_count

                    if new_phenomenon not in phen_list:
                        phen_list.append(new_phenomenon)


                gapi.grib_release(gid)

            fd.close()

            return phen_list

        except Exception:
            return None

    def get_metadata_grib_level2(self):

        file_info = self.get_metadata_generic_level1()

        if file_info is not None:

            #level 2.
            grib_phenomena = self.get_phenomena()

            self.handler_id = "grib handler level 2."

            if grib_phenomena is None:
                return file_info

            return  file_info +  (grib_phenomena, )

        else:
            return None

    def get_metadata_level3(self):

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

        lat_f_l = []
        lon_f_l = []
        lat_l_l = []
        lon_l_l = []
        date_d_l = []
        date_t_l = []

        phen_keys = [
                      "paramId",
                      "cfNameECMF",
                      "cfName",
                      "cfVarName",
                      "units",
                      "nameECMF",
                      "name",
                      "Ni",
                      "Nj",
                      "latitudeOfFirstGridPointInDegrees",
                      "longitudeOfFirstGridPointInDegrees",
                      "latitudeOfLastGridPointInDegrees",
                      "longitudeOfLastGridPointInDegrees",
                      "dataDate",
                      "dataTime"
                    ]
        try:
            fd = open(self.file_path)

            while 1:
                gid = gapi.grib_new_from_file(fd)
                if gid is None: break

                phen_attr_list = []
                attr_count = 0
                for key in phen_keys:

                    if not gapi.grib_is_defined(gid, key):
                        continue

                    value = str(gapi.grib_get(gid, key))

                    #So the file contains many records but all report the
                    #same spatial and temporal information. Only complete distinct records 
                    #will be stored i.e the one that contain the full list of parameter
                    #and are unique. If evety record has got different spatial and temporal
                    #then th eindex must change because currently there is only on geo_shape_field.
                    if key == "latitudeOfFirstGridPointInDegrees":
                        lat_f_l.append(value)
                    elif key == "longitudeOfFirstGridPointInDegrees":
                        lon_f_l.append(value)
                    elif key == "latitudeOfLastGridPointInDegrees":
                        lat_l_l.append(value)
                    elif key =="longitudeOfLastGridPointInDegrees":
                        lon_l_l.append(value)
                    elif key == "dataDate":
                        date_d_l.append(value)
                    elif key == "dataTime":
                        date_t_l.append(value)
                    else:
                        if    len(key) < util.MAX_ATTR_LENGTH \
                          and len(value) < util.MAX_ATTR_LENGTH \
                          and util.is_valid_phen_attr(value):

                            phen_attr["name"] = str(key.strip())
                            phen_attr["value"] = str(unicode(value).strip())

                            if phen_attr not in phen_attr_list:
                                phen_attr_list.append(phen_attr.copy())
                                attr_count = attr_count + 1

                if len(phen_attr_list) > 0:
                    new_phenomenon = phenomenon.copy() 
                    new_phenomenon["attributes"] = phen_attr_list
                    new_phenomenon["attribute_count"] = attr_count

                    if new_phenomenon not in phen_list:
                        phen_list.append(new_phenomenon)

                gapi.grib_release(gid)

            fd.close()

            if len(lat_f_l) > 0 \
               and len(lon_f_l) > 0  \
               and len(lat_l_l) > 0  \
               and len(lon_l_l) > 0  \
               and len(date_d_l) > 0 \
               and len(date_t_l):

                geospatial_dict = {}
                geospatial_dict["type"] = "envelope"

                temporal_dict = {} 
                lat_f = min(lat_f_l)
                lon_f = min(lon_f_l)
                lat_l = max(lat_l_l)
                lon_l = max(lon_l_l)

                date_d = min(date_d_l)
                date_t = min(date_t_l)

                if float(lon_l) > 180:
                    lon_l = (float(lon_l) -180) - 180

                geospatial_dict["coordinates"] = [[lat_f, lon_f], [lat_l, lon_l]]

                temporal_dict["start_time"] = date_d
                temporal_dict["end_time"] = date_t

                return (phen_list, geospatial_dict, temporal_dict)
            else:
                return (phen_list,)

        except Exception:
            return None

    def get_metadata_grib_level3(self):
        """
        Wrapper for method phenomena().
        :returns:  A dict containing information compatible with current es index level 2.
        """

        file_info = self.get_metadata_generic_level1()

        if file_info is not None:

            #level 2.
            metadata = self.get_metadata_level3()

            self.handler_id = "grib handler level 3."

            if metadata is None:
                return file_info

            if len(metadata) == 3:
                loc_dict = {}
                loc_dict["coordinates"] = metadata[1]
                file_info[0]["info"]["spatial"] = loc_dict
                file_info[0]["info"]["temporal"] = metadata[2]

            return file_info + (metadata[0], ) 

        else:
            return None

    def get_metadata(self):

        if self.level == "1":
            res = self.get_metadata_generic_level1()
        elif self.level == "2":
            res = self.get_metadata_grib_level2()
        elif self.level == "3":
            res = self.get_metadata_grib_level3()

        #Sice file format is decided it can be added. 
        res[0]["info"]["format"] = self.FILE_FORMAT

        return res

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass
