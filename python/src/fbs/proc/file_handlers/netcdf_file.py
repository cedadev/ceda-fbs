import netCDF4
from fbs.proc.file_handlers.generic_file import GenericFile
import fbs.proc.common_util.util as util
import fbs.proc.common_util.geojson as geojson
import six
from dateutil.parser import parse
import re
import logging
import traceback

logger = logging.getLogger(__name__)


def time_order(time1, time2):
    """
    Make sure that the times are the correct way round
    where start time is before end time.

    :param time1: First time input
    :param time2: Second time input

    :return: start_time, end_time
    """
    start_time = time1 if time1 < time2 else time2
    end_time = time2 if time2 > time1 else time1

    return start_time, end_time


def sanitise_float(value):
    """
    Clean text input for the lat and lon values
    Make sure that we return a float value.

    Some cases the extracted value is of the form: -90.0f; // float
    :param value:
    :return:
    """

    try:
        return float(value)
    except ValueError:
        # Clean all non-numeric characters
        value = re.sub('[^-.\d]', '', str(value))

    return float(value)


class NetCdfFile(GenericFile):
    """
    Simple class for returning basic information about the content
    of an NetCDF file.
    """

    def __init__(self, file_path, level, additional_param=None, **kwargs):
        GenericFile.__init__(self, file_path, level, **kwargs)
        self.FILE_FORMAT = "NetCDF"
        self.es = additional_param

    @staticmethod
    def clean_coordinate(coord):
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

        lat_min = getattr(ncdf, 'geospatial_lat_min', None)
        lat_max = getattr(ncdf, 'geospatial_lat_max', None)
        lon_min = getattr(ncdf, 'geospatial_lon_min', None)
        lon_max = getattr(ncdf, 'geospatial_lon_max', None)

        if all([lat_min, lat_max, lon_min, lon_max]):
            return {
                "type": "track",
                "lat": [sanitise_float(lat_min), sanitise_float(lat_max)],
                "lon": [sanitise_float(lon_min), sanitise_float(lon_max)]
            }

        lats = ncdf.variables[lat_name][:].ravel()
        lons = ncdf.variables[lon_name][:].ravel()
        return {
            "type": "track",
            "lat": lats,
            "lon": lons
        }

    def find_var_by_standard_name(self, ncdf, standard_name):
        """
        Find a variable reference searching by CF standard name.

        :param Dataset ncdf: Reference to an opened netCDF4.Dataset object
        :param str standard_name: The CF standard name to search for
        """
        for key, value in six.iteritems(ncdf.variables):
            try:
                if value.standard_name.lower() == standard_name.lower():
                    return key
            except AttributeError:
                continue

    def get_geospatial(self, ncdf):
        lat_name = self.find_var_by_standard_name(ncdf, "latitude")
        lon_name = self.find_var_by_standard_name(ncdf, "longitude")

        if lat_name and lon_name:
            return self.geospatial(ncdf, lat_name, lon_name)

    def get_temporal(self, ncdf):

        temporal = {}

        time_name = self.find_var_by_standard_name(ncdf, "time")
        times = None

        # Try time coverage attributes
        start_time = getattr(ncdf, 'time_coverage_start', None)
        end_time = getattr(ncdf, 'time_coverage_end', None)

        # If extracting from the header not successful, try to retrieve the time
        # coordinate, if we have a coordinate name
        if not all([start_time, end_time]) and time_name:
            try:
                times = list(netCDF4.num2date(list(ncdf.variables[time_name]),
                                              ncdf.variables[time_name].units))
            except AttributeError:
                pass

        # retrieve the start and end times
        if start_time:
            start_time = parse(start_time)
        elif times:
            start_time = times[0]

        if end_time:
            end_time = parse(end_time)
        elif times:
            end_time = times[-1]

        # Check the order if we have a start and end
        if all([start_time, end_time]):
            start_time, end_time = time_order(start_time, end_time)

        if start_time and not end_time:
            end_time = start_time

        # Build the response
        if start_time:
            temporal = {
                "time_range": {
                    "gte": start_time.isoformat()
                },
                "start_time": start_time.isoformat()
            }

        # Need to use get because it is possible that a start date
        # was not found
        if end_time:
            time_range = temporal.get('time_range', {})
            time_range.update({
                "lte": end_time.isoformat()
            })
            temporal["end_time"] = end_time.isoformat()

        return temporal

    def get_phenomena(self, netcdf):
        """
        Construct list of Phenomena based on variables in NetCDF file.
        :returns : List of metadata.product.Parameter objects.
        """
        phen_list = []

        # for all phenomena list.
        for v_name, v_data in six.iteritems(netcdf.variables):
            new_phenomenon = {}
            phen_attr_list = []

            # for all attributtes in phenomenon.
            for key, value in six.iteritems(vars(v_data)):

                if not util.is_valid_phenomena(key, value):
                    continue

                phen_attr = dict(
                    name=str(key.strip()),
                    value=(value.strip()).encode('utf-8', 'ignore').decode()
                )
                phen_attr_list.append(phen_attr)

            # Add the variable ID
            phen_attr = dict(name='var_id', value=str(v_name))
            phen_attr_list.append(phen_attr)

            # Extract extra information about each variable not in the __dict__ response
            for attr in ["dtype", "shape", "_ChunkSizes", "_FillValue", "dimensions", "size"]:
                try:
                    value = getattr(v_data, attr)

                    if attr == 'dtype':
                        value = value.name

                    # Convert value datatype to JSON compatible
                    if type(value) not in (str, tuple, list):
                        value = str(value)

                    phen_attr = dict(name=attr, value=value)
                    phen_attr_list.append(phen_attr)

                except AttributeError:
                    pass

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
                return file_info + netcdf_phenomena
            except Exception:
                file_info[0]["info"]["read_status"] = "Read Error"

                return file_info + (None,)

    def get_metadata_level3(self):

        """
        Wrapper for method phenomena().
        :returns:  A dict containing information compatible with current es index level 2.
        """
        # level 1
        file_info = self.get_metadata_level1()
        spatial = None
        netcdf_phenomena = None

        if file_info is not None:

            try:
                with netCDF4.Dataset(self.file_path) as netcdf:

                    # level 2
                    netcdf_phenomena = self.get_phenomena(netcdf)

                    self.handler_id = "Netcdf handler level 3."

                    # try to add level 3 info.
                    try:
                        geo_info = self.get_geospatial(netcdf)

                        if geo_info:

                            spatial = None

                            gj = geojson.GeoJSONGenerator(geo_info["lat"], geo_info["lon"])
                            spatial = gj.get_elasticsearch_geojson()
                            if spatial:
                                spatial = {"coordinates": spatial["geometries"]["search"]}

                    except AttributeError as ex:
                        pass

                    try:
                        temp_info = self.get_temporal(netcdf)

                        if temp_info:
                            file_info[0]["info"]["temporal"] = temp_info
                    except (KeyError, AttributeError):
                        pass

                    file_info[0]["info"]["read_status"] = "Successful"
                    return file_info + netcdf_phenomena + (spatial,)
            except Exception as ex:
                logger.error(traceback.format_exc())
                if netcdf_phenomena is not None:

                    file_info[0]["info"]["read_status"] = "Successful"
                    return file_info + netcdf_phenomena
                else:
                    file_info[0]["info"]["read_status"] = "Read Error"
                    return file_info

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
        file = sys.argv[2]
    except IndexError:
        level = '1'
        file = '/badc/mst/data/nerc-mstrf-radar-mst/v4-0/st-mode/cardinal/2015/09/nerc-mstrf-radar-mst_capel-dewi_20150901_st300_cardinal_33min-smoothing_v4-0.nc'

    ncf = NetCdfFile(file, level)
    start = datetime.datetime.today()
    print(ncf.get_metadata())
    end = datetime.datetime.today()
    print(end - start)
