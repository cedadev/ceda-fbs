import netCDF4

from proc.file_handlers.generic_file import GenericFile
import proc.common_util.util as util
import proc.common_util.geojson as geojson

class   NetCdfFile(GenericFile):
    """
    Simple class for returning basic information about the content
    of an NetCDF file.
    """

    def __init__(self, file_path, level, additional_param=None):
        GenericFile.__init__(self, file_path, level)
        self.FILE_FORMAT = "NetCDF"
        self.es = additional_param

    def get_handler_id(self):
        return self.handler_id

    def clean_coordinate(self, coord):
        """Return True if coordinate is valid."""
        try:
            # This filters out misconfigured "_FillValue" elements
            if coord == 0.0:
                return False

            int(coord)  # If this fails, "coord" is not a number!

            return True
        except ValueError:
            return False


    def geospatial(self, ncdf, lat_name, lon_name):
        """
        Return a dict containing lat/lons from NetCDF file.

        :param Dataset ncdf: Reference to an opened netcdf4.Dataset object
        :param lat_name: Name of parameter containing latitude values
        :param lon_name: Name of parameter containing longitude values
        :returns: Geospatial information as dict.
        """

        # Filter out items that are equal to "masked"
        lats = filter(self.clean_coordinate,
                      ncdf.variables[lat_name][:].ravel())
        lons = filter(self.clean_coordinate,
                      ncdf.variables[lon_name][:].ravel())
        return {
            "type": "track",
            "lat": lats,
            "lon": lons
        }


    def find_var_by_standard_name(self, ncdf, fpath, standard_name):
        """
        Find a variable reference searching by CF standard name.

        :param Dataset ncdf: Reference to an opened netCDF4.Dataset object
        :param str standard_name: The CF standard name to search for
        """
        for key, value in ncdf.variables.iteritems():
            try:
                if value.standard_name.lower() == standard_name.lower():
                    return key
            except AttributeError:
                continue

        return None

    #ok lets try something new.
    def get_geospatial(self, ncdf):
        lat_name = self.find_var_by_standard_name(ncdf, self.file_path, "latitude")
        lon_name = self.find_var_by_standard_name(ncdf, self.file_path, "longitude")

        if lat_name and lon_name:
            return self.geospatial(ncdf, lat_name, lon_name)
        else:
            return None

    def temporal(self, ncdf, time_name):
        """
        Extract time values from Dataset using the variable name provided.

        :param Dataset ncdf: Reference to an opened netcdf4.Dataset object
        :param str time_name: Name of the time parameter
        """
        times = list(netCDF4.num2date(list(ncdf.variables[time_name]),
                                      ncdf.variables[time_name].units))
        return {
            "start_time": times[0].isoformat(),
            "end_time": times[-1].isoformat()
        }

    def get_temporal(self, ncdf):
        time_name = self.find_var_by_standard_name(ncdf, self.file_path, "time")
        return self.temporal(ncdf, time_name) 

    # def is_valid_parameter(self, name, value):
    #     valid_parameters = [ "standard_name",
    #                          "long_name",
    #                          "title",
    #                          "name",
    #                          "units"
    #                        ]
    #     if name in valid_parameters \
    #        and len(value) < util.MAX_ATTR_LENGTH\
    #        and len(name) < util.MAX_ATTR_LENGTH:
    #         return True
    #     return False
    #
    # def is_valid_phenomena(self,key,value):
    #     """
    #     Wrapper to hide test in main function
    #     """
    #
    #     if not self.is_valid_parameter(key, value):
    #         return False
    #
    #     if not util.is_valid_phen_attr(value):
    #         return False
    #
    #     # Returns true if both the tests above pass as true
    #     return True

    def get_phenomena(self, netcdf):
        """
        Construct list of Phenomena based on variables in NetCDF file.
        :returns : List of metadata.product.Parameter objects.
        """
        phen_list = []

        #for all phenomena list.
        for v_name, v_data in netcdf.variables.iteritems():
            new_phenomenon = {}
            phen_attr_list = []

            #for all attributtes in phenomenon.
            for key, value in v_data.__dict__.iteritems():

                if not util.is_valid_phenomena(key,value):
                    continue

                phen_attr = {}

                phen_attr["name"] = str(key.strip())
                phen_attr["value"] = str(unicode(value).strip())

                phen_attr_list.append(phen_attr)


            phen_attr["name"] = "var_id"
            phen_attr["value"] = str(v_name)

            phen_attr_list.append(phen_attr)

            if len(phen_attr_list) > 0:
                new_phenomenon["attributes"] = phen_attr_list

                phen_list.append(new_phenomenon)

        file_phenomena = util.build_phenomena(phen_list)

        return file_phenomena

    def get_metadata_level2(self):

        file_info = self.get_metadata_level1()

        if file_info is not None:
            try:
                with netCDF4.Dataset(self.file_path) as netcdf_object:
                    netcdf_phenomena = self.get_phenomena(netcdf_object)
                    file_info[0]["info"]["read_status"] = "Successful"
                return file_info +  netcdf_phenomena
            except Exception:
                file_info[0]["info"]["read_status"] = "Read Error"

                return file_info + (None,)
        else:
            return None

    def get_metadata_level3(self):

        """
        Wrapper for method phenomena().
        :returns:  A dict containing information compatible with current es index level 2.
        """
        #level 1
        file_info = self.get_metadata_level1()
        spatial = None
        netcdf_phenomena = None

        if file_info is not None:

            try:
                with netCDF4.Dataset(self.file_path) as netcdf:

                    #level 2
                    netcdf_phenomena = self.get_phenomena(netcdf)

                    self.handler_id = "Netcdf handler level 3."

                    #try to add level 3 info. 
                    try:
                        geo_info = self.get_geospatial(netcdf)

                        loc_dict= {}

                        gj = geojson.GeoJSONGenerator(geo_info["lat"], geo_info["lon"])
                        spatial = gj.get_elasticsearch_geojson()

                        loc_dict["coordinates"]= spatial["geometries"]["search"]#["coordinates"]
                        spatial = loc_dict
                    except AttributeError:
                        pass

                    try:
                        temp_info = self.get_temporal(netcdf)
                        file_info[0]["info"]["temporal"] = temp_info
                    except AttributeError:
                        pass

                    file_info[0]["info"]["read_status"] = "Successful"

                    return file_info  + netcdf_phenomena + (spatial, )
            except Exception as ex:
                if netcdf_phenomena is not None:

                    file_info[0]["info"]["read_status"] = "Successful"
                    return file_info + (netcdf_phenomena, )
                else:
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

    file = '/badc/accmip/data/GISS/GISS-E2-R/accrcp45/ACCMIP-monthly/r1i1p3/v1/rsutcs/rsutcs_ACCMIP-monthly_GISS-E2-R_accrcp45_r1i1p3_207101-207112.nc'
    ncf = NetCdfFile(file,level)
    start = datetime.datetime.today()
    print ncf.get_metadata()
    end = datetime.datetime.today()
    print end-start

