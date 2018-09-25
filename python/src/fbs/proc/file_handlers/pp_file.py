import cdms2 as cdms
import proc.common_util.util as util


from proc.file_handlers.generic_file import GenericFile

class PpFile(GenericFile):
    """
    Simple class for returning basic information about the content
    of an PP file.
    """

    def __init__(self, file_path, level, additional_param=None, **kwargs):
        GenericFile.__init__(self, file_path, level, **kwargs)
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

    def get_phenomena(self, pp_file_content):

        try:
            self.handler_id = "pp handler level 2."
            # pp_file_content=cdms.open(self.file_path)
            var_ids = pp_file_content.listvariables()

            # Filter long values and overwrite duplicates.
            phen_list = []
            for var_id in var_ids:
                new_phenomenon = {}
                metadata_dict = pp_file_content[var_id].attributes
                phen_attr_list = []
                for key in metadata_dict.keys():
                    value = str(metadata_dict[key])

                    if not util.is_valid_phenomena(key,value):
                        continue

                    phen_attr = {}

                    phen_attr["name"] = str(key.strip())
                    phen_attr["value"] = str(unicode(value).strip())

                    phen_attr_list.append(phen_attr)

                # Dict of phenomenon attributes.
                if len(phen_attr_list) > 0:
                    new_phenomenon["attributes"] = phen_attr_list

                    # Append to list if new_phenomenon is NOT already in the phen_list
                    if new_phenomenon not in phen_list:
                        phen_list.append(new_phenomenon)

            # pp_file_content.close()

            file_phenomena = util.build_phenomena(phen_list)

            return file_phenomena
        except Exception as ex:
            return None

    def get_metadata_level2(self):

        # Get basic file info.
        file_info = self.get_metadata_level1()

        if file_info is not None:
            try:
                pp_file_content = cdms.open(self.file_path)
                phen_list = self.get_phenomena(pp_file_content)
                pp_file_content.close()

                if phen_list is not None:
                    file_info[0]["info"]["read_status"] = "Successful"
                    return file_info + phen_list
                else:
                    file_info[0]["info"]["read_status"] = "Read Error"
                    return file_info + (None,)

            except Exception:
                file_info[0]["info"]["read_status"] = "Read Error"
                return file_info

        else:
            return None

    def normalize_lon(self, coord):
        if coord > 180:
            coord = coord - 360
        return coord

    def normalize_lat(self, coord):
        if coord > 90:
            coord = 90
        if coord < -90:
            coord = 90

        return coord

    def get_metadata_level3(self):

        spatial = None
        #Get basic file info.
        file_info = self.get_metadata_level1()

        if file_info is not None:
            try:
                self.handler_id = "pp handler level 3."
                lat_l = []
                lat_u = []
                lon_l = []
                lon_u = []
                start_time_l = []
                end_time_l = []

                pp_file_content=cdms.open(self.file_path)
                var_ids = pp_file_content.listvariables()

                for var_id in var_ids:
                    try:
                        spatial  = self.getBoundingBox(var_id, pp_file_content)
                        temporal = self.getTemporalDomain(var_id, pp_file_content)

                        # Geospatial data
                        lon_l.append(spatial[0])
                        lat_l.append(spatial[1])
                        lon_u.append(spatial[2])
                        lat_u.append(spatial[3])

                        # Temporal data
                        start_time_l.append(temporal[0])
                        end_time_l.append(temporal[1])

                    except Exception as ex:
                        continue

                # Make sure that there are values in all the lists
                if all(v for v in [lat_l,lon_l,lat_u,lon_u]):

                    min_lon_l = self.normalize_lon(min(lon_l))
                    min_lat_l = self.normalize_lat(min(lat_l))
                    max_lon_u = self.normalize_lon(max(lon_u))
                    max_lat_u = self.normalize_lat(max(lat_u))

                    spatial = {'coordinates': {'type': 'envelope', 'coordinates': [[round(min_lon_l, 3), round(min_lat_l, 3)], [round(max_lon_u, 3), round(max_lat_u, 3)]]}}

                if start_time_l and end_time_l:
                    min_time = min(start_time_l)
                    max_time = max(end_time_l)

                    if util.is_date_valid(min_time.split("T")[0])\
                       and util.is_date_valid(max_time.split("T")[0]):
                        file_info[0]["info"]["temporal"] = {'start_time': min_time, 'end_time': max_time }

                phen_list = self.get_phenomena(pp_file_content)

                pp_file_content.close()


                file_info[0]["info"]["read_status"] = "Successful"

                return file_info + phen_list + (spatial, )

            except Exception as ex:
                # There was an error reading the file
                file_info[0]["info"]["read_status"] = "Read Error"
                return file_info
        else:
            return None

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

    file = "/badc/amma/data/ukmo-nrt/africa-lam/pressure_level_split/af/fp/2006/07/02/affp2006070218_05201_33.pp"
    ppf = PpFile(file,level)
    start = datetime.datetime.today()
    print ppf.get_metadata()
    end = datetime.datetime.today()
    print end-start