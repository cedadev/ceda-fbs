import fbs.proc.common_util.util as util
from fbs.proc.file_handlers.generic_file import GenericFile
import six
import numpy as np
import cf


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

    @staticmethod
    def get_bounding_box(file):
        """
        Returns the horizontal domain as (north, south, east, west)
        """
        lats = file.coord('latitude').data
        north = lats.max()
        south = lats.min()

        lons = file.coord('longitude').data
        east = lons.max()
        west = lons.min()

        directions = []
        for direction in north, south, east, west:
            directions.append(direction.array[0])

        return directions


    @staticmethod
    def get_temporal_domain(file):
        """
        Returns the temporal domain as a tuple of start_time, end_time.
        """

        time = file.dimension_coordinate('time')
        dates = np.sort(time.dtarray)

        start_time = dates[0].strftime('%Y-%m-%dT%H:%M:%S')
        end_time = dates[-1].strftime('%Y-%m-%dT%H:%M:%S')

        return start_time, end_time

    @staticmethod
    def normalize_lon(coord):
        if coord > 180:
            coord = coord - 360
        return coord

    @staticmethod
    def normalize_lat(coord):
        if coord > 90:
            coord = 90

        if coord < -90:
            coord = 90

        return coord

    def get_phenomena(self, pp_files):

        try:
            self.handler_id = "pp handler level 2."

            phen_list = []
            for file in pp_files:
                metadata = file.properties()
                phen_attr_list = []
                for key, value in six.iteritems(metadata):
                    value = str(value)
                    if not util.is_valid_phenomena(key, value):
                        continue

                    phen_attr = {
                        "name": str(key.strip()),
                        "value": value.strip()
                    }

                    phen_attr_list.append(phen_attr)

                # Dict of phenomenon attributes.
                if phen_attr_list:
                    new_phenomenon = {
                        "attributes": phen_attr_list
                    }

                    # Append to list if new_phenomenon is NOT already in the phen_list
                    if new_phenomenon not in phen_list:
                        phen_list.append(new_phenomenon)

            file_phenomena = util.build_phenomena(phen_list)

            return file_phenomena

        except Exception as ex:
            raise ex
            # return None

    def get_metadata_level2(self):

        # Get basic file info.
        file_info = self.get_metadata_level1()

        if file_info is not None:
            try:
                pp_files = cf.read(self.file_path)
                phen_list = self.get_phenomena(pp_files)

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

    def get_metadata_level3(self):

        spatial = None
        # Get basic file info.
        file_info = self.get_metadata_level1()

        if file_info is not None:
            try:
                self.handler_id = "pp handler level 3."
                north = []
                south = []
                east = []
                west = []

                start_time_list = []
                end_time_list = []

                pp_files = cf.read(self.file_path)
                for file in pp_files:
                    try:
                        n, s, e, w = self.get_bounding_box(file)
                        start, end = self.get_temporal_domain(file)

                        # Geospatial data
                        north.append(n)
                        south.append(s)
                        east.append(e)
                        west.append(w)

                        # Temporal data
                        start_time_list.append(start)
                        end_time_list.append(end)

                    except Exception as ex:
                        print(ex)
                        continue

                # Make sure that there are values in all the lists
                if all(v for v in [north, south, east, west]):
                    min_lon = self.normalize_lon(min(west))
                    min_lat = self.normalize_lat(min(south))
                    max_lon = self.normalize_lon(max(east))
                    max_lat = self.normalize_lat(max(north))

                    spatial = {
                        "coordinates": {
                            "type": "envelope",
                            "coordinates": [
                                [round(float(min_lon), 3), round(float(min_lat), 3)],
                                [round(float(max_lon), 3), round(float(max_lat), 3)]
                            ]
                        }
                    }

                if start_time_list and end_time_list:
                    min_time = min(start_time_list)
                    max_time = max(end_time_list)

                    file_info[0]["info"]["temporal"] = {
                        "time_range": {
                            "gte": min_time,
                            "lte": max_time
                        },
                        "start_time": min_time,
                        "end_time": max_time
                    }

                phen_list = self.get_phenomena(pp_files)

                file_info[0]["info"]["read_status"] = "Successful"

                return file_info + phen_list + (spatial,)

            except Exception as ex:
                # There was an error reading the file
                file_info[0]["info"]["read_status"] = "Read Error"
                return file_info
        else:
            return None

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
        level = '3'
        file = "/badc/amma/data/ukmo-nrt/africa-lam/pressure_level_split/af/fp/2006/07/02/affp2006070218_05201_33.pp"

    ppf = PpFile(file, level)
    start = datetime.datetime.today()
    print(ppf.get_metadata())
    end = datetime.datetime.today()
    print(end - start)

