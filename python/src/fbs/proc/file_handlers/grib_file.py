import gribapi as gapi
import fbs.proc.common_util.util as util
from fbs.proc.file_handlers.generic_file import GenericFile

class GribFile(GenericFile):
    """
    Class for returning basic information about the content
    of an Grib file.
    """

    def __init__(self, file_path, level, additional_param=None, **kwargs):
        GenericFile.__init__(self, file_path, level, **kwargs)
        self.FILE_FORMAT = "GRIB"

    def get_handler_id(self):
        return self.handler_id

    def get_phenomena(self):

        phen_list = []
        phen_keys = [
            "paramId",
            "cfNameECMF",
            "cfName",
            "cfVarName",
            "units",
            "nameECMF",
            "name"
        ]

        try:
            with open(self.file_path) as fd:
                while True:
                    gid = gapi.grib_new_from_file(fd)
                    if gid is None: break

                    phen_attr_list = []
                    new_phenomenon = {}
                    for key in phen_keys:
                        phen_attr = {}

                        if not gapi.grib_is_defined(gid, key):
                            continue

                        value = str(gapi.grib_get(gid, key))
                        if util.is_valid_phenomena(key,value):

                            phen_attr["name"] = str(key.strip())
                            phen_attr["value"] = str(unicode(value).strip())

                            if phen_attr not in phen_attr_list:
                                phen_attr_list.append(phen_attr)

                    if len(phen_attr_list) > 0:
                        new_phenomenon["attributes"] = phen_attr_list

                        if new_phenomenon not in phen_list:
                            phen_list.append(new_phenomenon)

                    gapi.grib_release(gid)

            file_phenomena = util.build_phenomena(phen_list)

            return file_phenomena

        except Exception:
            return None


    def get_geospatial(self):


        lat_f_l = []
        lon_f_l = []
        lat_l_l = []
        lon_l_l = []

        phen_keys = [
            "latitudeOfFirstGridPointInDegrees",
            "longitudeOfFirstGridPointInDegrees",
            "latitudeOfLastGridPointInDegrees",
            "longitudeOfLastGridPointInDegrees",
        ]
        with open(self.file_path) as fd:
            while True:

                gid = gapi.grib_new_from_file(fd)
                if gid is None: break

                for key in phen_keys:

                    # move on if key not found
                    if not gapi.grib_is_defined(gid, key):
                        continue

                    value = str(gapi.grib_get(gid, key))

                    # So the file contains many records but all report the
                    # same spatial and temporal information. Only complete distinct records
                    # will be stored i.e the one that contain the full list of parameter
                    # and are unique. If every record has got different spatial and temporal
                    # then th eindex must change because currently there is only on geo_shape_field.
                    if key == "latitudeOfFirstGridPointInDegrees":
                        lat_f_l.append(value)
                    elif key == "longitudeOfFirstGridPointInDegrees":
                        lon_f_l.append(value)
                    elif key == "latitudeOfLastGridPointInDegrees":
                        lat_l_l.append(value)
                    elif key == "longitudeOfLastGridPointInDegrees":
                        lon_l_l.append(value)

                gapi.grib_release(gid)

        if len(lat_f_l) > 0 \
                and len(lon_f_l) > 0 \
                and len(lat_l_l) > 0 \
                and len(lon_l_l) > 0:

            geospatial_dict = {}
            geospatial_dict["type"] = "envelope"

            lat_f = min(lat_f_l)
            lon_f = min(lon_f_l)
            lat_l = max(lat_l_l)
            lon_l = max(lon_l_l)


            if float(lon_l) > 180:
                lon_l = (float(lon_l) - 180) - 180

            geospatial_dict["coordinates"] = [[round(float(lon_f), 3), round(float(lat_f), 3)],
                                              [round(float(lon_l), 3), round(float(lat_l), 3)]]

            loc_dict = {"coordinates": geospatial_dict}

            return (loc_dict,)

        else:
            return (None,)

    def get_temporal(self):

        date_d_l = []
        date_t_l = []

        phen_keys = [
            "dataDate",
            "dataTime"
        ]

        temporal_dict = {}
        try:
            with open(self.file_path) as fd:
                while True:
                    gid = gapi.grib_new_from_file(fd)
                    if gid is None: break

                    for key in phen_keys:

                        if not gapi.grib_is_defined(gid, key):
                            continue

                        value = str(gapi.grib_get(gid, key))

                        # So the file contains many records but all report the
                        # same spatial and temporal information. Only complete distinct records
                        # will be stored i.e the one that contain the full list of parameter
                        # and are unique. If every record has got different spatial and temporal
                        # then the index must change because currently there is only on geo_shape_field.
                        if key == "dataDate":
                            date_d_l.append(value)
                        elif key == "dataTime":
                            date_t_l.append(value)

                    gapi.grib_release(gid)

            if len(date_d_l) > 0 \
                    and len(date_t_l):

                date_d = min(date_d_l)
                date_t = min(date_t_l)

                date_dm = max(date_d_l)
                date_tm = max(date_t_l)

                if len(date_t) != 4:
                    dt_min = datetime.datetime.strptime(date_d, '%Y%m%d').isoformat()
                else:
                    dt_min = datetime.datetime.strptime(date_d + date_t, '%Y%m%d%H%M').isoformat()

                if len(date_tm) != 4:
                    dt_max = datetime.datetime.strptime(date_dm, '%Y%m%d').isoformat()
                else:
                    dt_max = datetime.datetime.strptime(date_dm + date_tm, '%Y%m%d%H%M').isoformat()

                temporal_dict["start_time"] = dt_min
                temporal_dict["end_time"] = dt_max

                return temporal_dict
            else:
                return None

        except Exception as ex:
            return None

    def get_metadata_level2(self):

        file_info = self.get_metadata_level1()

        if file_info is not None:

            # level 2.
            grib_phenomena = self.get_phenomena()

            self.handler_id = "grib handler level 2."
            try:
                if grib_phenomena is None:
                    file_info[0]["info"]["read_status"] = "Read Error"
                    return file_info + (None,)

                else:
                    # todo Change this so that errors proagate back up and are caugh, not just hidden by a None response.
                    file_info[0]["info"]["read_status"] = "Successful"
                    return file_info + grib_phenomena
            except Exception:
                file_info[0]["info"]["read_status"] = "Read Error"
                return file_info + (None,)
        else:
            return None

    def get_metadata_level3(self):
        """
        :returns:  A dict containing level 3 file metadata.
        """
        file_info = self.get_metadata_level1()

        geospatial = self.get_geospatial()
        phenomena = self.get_phenomena()
        temporal = self.get_temporal()

        if file_info is not None:
            self.handler_id = "grib handler level 3."
            file_info[0]["info"]["read_status"] = "Successful"

            if temporal is not None:
                file_info[0]["info"]["temporal"] = temporal

            return file_info + phenomena + geospatial

        else:
            return None

    def get_metadata(self):

        if self.level == "1":
            res = self.get_metadata_level1()
        elif self.level == "2":
            res = self.get_metadata_level2()
        elif self.level == "3":
            res = self.get_metadata_level3()

        # Sice file format is decided it can be added.
        res[0]["info"]["format"] = self.FILE_FORMAT

        return res

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


if __name__ == "__main__":
    import datetime
    import sys
    import timeit

    # run test
    try:
        level = str(sys.argv[1])
    except IndexError:
        level = '1'

    try:
        file = sys.argv[2]
    except IndexError:
        file = '/badc/ecmwf-for/slimcat/data/2012/11/spam2012110318u.grb'

    grf = GribFile(file, level)
    start = datetime.datetime.today()
    print grf.get_metadata()
    end = datetime.datetime.today()
    print end-start