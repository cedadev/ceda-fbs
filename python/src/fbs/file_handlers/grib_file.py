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
                        break

                    value = str(gapi.grib_get(gid, key))
                    if len(key) < util.MAX_ATTR_LENGTH \
                       and len(value) < util.MAX_ATTR_LENGTH:

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

        lat_f = None
        lon_f = None
        lat_l = None
        lon_l = None
        date_d = None
        date_t = None

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
                        lat_f = None
                        lon_f = None
                        lat_l = None
                        lon_l = None
                        date_d = None
                        date_t = None
                        break

                    value = str(gapi.grib_get(gid, key))

                    #So the file contains many records but all report the
                    #same spatial and temporal information. Only complete distinct records 
                    #will be stored i.e the one that contain the full list of parameter
                    #and are unique. If evety record has got different spatial and temporal
                    #then th eindex must change because currently there is only on geo_shape_field.
                    if key == "latitudeOfFirstGridPointInDegrees" and lat_f is None:
                        lat_f = value
                    elif key == "longitudeOfFirstGridPointInDegrees" and lon_f is None:
                        lon_f = value
                    elif key == "latitudeOfLastGridPointInDegrees" and lat_l is None:
                        lat_l = value
                    elif key =="longitudeOfLastGridPointInDegrees" and lon_l is None:
                        lon_l = value
                    elif key == "dataDate" and date_d is None:
                        date_d = value
                    elif key == "dataTime" and date_t is None:
                        date_t = value
                    else:
                        if    len(key) < util.MAX_ATTR_LENGTH \
                          and len(value) < util.MAX_ATTR_LENGTH:

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

            geospatial_dict = {}
            temporal_dict = {}
            if lat_f is not None      \
               and lon_f is not None  \
               and lat_l is not None  \
               and lon_l is not None  \
               and date_d is not None \
               and date_t is not None:

                geospatial_dict["type"] = "envelope"
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
                file_info[0]["spatial"] = loc_dict
                file_info[0]["temporal"] = metadata[2]

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
