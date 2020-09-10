# import gribapi as gapi
import fbs.proc.common_util.util as util
from fbs.proc.file_handlers.generic_file import GenericFile
import xarray as xr
import numpy as np


class GribFile(GenericFile):
    """
    Class for returning basic information about the content
    of an Grib file.
    """

    def __init__(self, file_path, level, additional_param=None, **kwargs):
        GenericFile.__init__(self, file_path, level, **kwargs)
        self.FILE_FORMAT = "GRIB"

    @staticmethod
    def get_phenomena(dataset):

        phen_list = []
        phen_keys = [
            "long_name",
            "units",
            "standard_name",
            "GRIB_name",
        ]

        try:

            # Loop variables
            for variable in dataset.data_vars:

                # Variable is a string, get the actual reference
                variable = dataset.data_vars[variable]
                phen_attr_list = []

                for key in phen_keys:

                    value = variable.attrs.get(key)

                    # Move on if key not present
                    if value is None:
                        continue

                    key = key.replace("GRIB_", "")

                    if util.is_valid_phenomena(key, value):

                        phen_attr = {
                            "name": key.strip(),
                            "value": value.strip()
                        }

                        if phen_attr not in phen_attr_list:
                            phen_attr_list.append(phen_attr)

                if phen_attr_list:
                    new_phenomenon = {
                        "attributes": phen_attr_list
                    }

                    if new_phenomenon not in phen_list:
                        phen_list.append(new_phenomenon)

            return util.build_phenomena(phen_list)

        except Exception:
            return

    @staticmethod
    def get_geospatial(dataset):
        """
        :param dataset: xarray dataset
        :return:
        """
        try:
            lat_min = dataset.latitude.data.min()
            lat_max = dataset.latitude.data.max()
            lon_min = dataset.longitude.data.min()
            lon_max = dataset.longitude.data.max()

            step = (lon_max - lon_min) / (dataset.longitude.data.shape[0] - 1)

            # Handle global grids
            if lon_max + step == 360 and lon_min == 0:
                lon_min = -180
                lon_max = 180

            # If we cannot assume that it is a 0 - 360 grid and the max is > 180
            # then it is ambiguous and so return None
            if lon_max > 180:
                return

            # Coordinates upper left (lon, lat) lower right (lon, lat)
            return {
                "coordinates":{
                    "coordinates": [
                        [lon_min, lat_max],
                        [lon_max, lat_min]
                    ]
                }
            }

        # An error occurred
        except Exception:
            return

    @staticmethod
    def get_temporal(dataset):
        """

        :param dataset: xarray dataset
        :return: Isodate range
        """
        try:
            ds_start = dataset.time.data.min()
            ds_end = dataset.time.data.max()

            return {
                "time_range": {
                    "gte": np.datetime_as_string(ds_start, unit='s'),
                    "lte": np.datetime_as_string(ds_end, unit='s')
                },
                "start_time": np.datetime_as_string(ds_start, unit='s'),
                "end_time": np.datetime_as_string(ds_end, unit='s')
            }

        except Exception:
            return

    def get_metadata_level2(self):

        file_info = self.get_metadata_level1()

        if file_info is not None:

            try:
                dataset = xr.open_dataset(self.file_path, engine='cfgrib', backend_kwargs={'indexpath': ''})
            except Exception as e:
                print(e)
                file_info[0]["info"]["read_status"] = "Read Error"
                return file_info + (None,)

            grib_phenomena = self.get_phenomena(dataset)

            self.handler_id = "grib handler level 2."
            if grib_phenomena is None:
                file_info[0]["info"]["read_status"] = "Read Error"
                return file_info + (None,)

            else:
                # todo Change this so that errors propagate back up and are caught, not just hidden by a None response.
                file_info[0]["info"]["read_status"] = "Successful"
                return file_info + grib_phenomena

        else:
            return None

    def get_metadata_level3(self):
        """
        :returns:  A dict containing level 3 file metadata.
        """
        file_info = self.get_metadata_level1()

        if file_info is not None:
            try:
                dataset = xr.open_dataset(self.file_path, engine='cfgrib', backend_kwargs={'indexpath': ''})
            except Exception:
                file_info[0]["info"]["read_status"] = "Read Error"
                return file_info + (None,)

            geospatial = self.get_geospatial(dataset)
            phenomena = self.get_phenomena(dataset)
            temporal = self.get_temporal(dataset)

            self.handler_id = "grib handler level 3."
            file_info[0]["info"]["read_status"] = "Successful"

            if temporal is not None:
                file_info[0]["info"]["temporal"] = temporal

            return file_info + phenomena + (geospatial,)

        else:
            return None

    def get_metadata(self):

        if self.level == "1":
            res = self.get_metadata_level1()
        elif self.level == "2":
            res = self.get_metadata_level2()
        elif self.level == "3":
            res = self.get_metadata_level3()

        # Since file format is decided it can be added.
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
        file = sys.argv[2]
    except IndexError:
        level = '1'
        file = '/badc/ecmwf-for/slimcat/data/2012/11/spam2012110318u.grb'

    grf = GribFile(file, level)
    start = datetime.datetime.today()
    print(grf.get_metadata())
    end = datetime.datetime.today()
    print(end - start)
