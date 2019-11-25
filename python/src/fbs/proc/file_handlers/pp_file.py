import iris
import fbs.proc.common_util.util as util
from fbs.proc.file_handlers.generic_file import GenericFile
import six
import numpy as np


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
    def getBoundingBox(cube):
        """
        Returns the horizontal domain as (north, south, east, west)
        """

        north = np.max(cube.coord('latitude').points)
        south = np.min(cube.coord('latitude').points)

        east = np.max(cube.coord('longitude').points)
        west = np.min(cube.coord('longitude').points)

        return north, south, east, west

    @staticmethod
    def getTemporalDomain(cube):
        """
        Returns the temporal domain as a tuple of start_time, end_time.
        """
        time = cube.coord('time')
        dates = time.units.num2date(np.sort(time.points))

        start_time = dates[0].isoformat()
        end_time = dates[-1].isoformat()

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

    def get_phenomena(self, pp_cubes):

        try:
            self.handler_id = "pp handler level 2."

            phen_list = []
            for cube in pp_cubes:
                metadata = cube.metadata._asdict()

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
                pp_cubes = iris.load(self.file_path)
                phen_list = self.get_phenomena(pp_cubes)

                if phen_list is not None:
                    file_info[0]["info"]["read_status"] = "Successful"
                    return file_info + phen_list
                else:
                    file_info[0]["info"]["read_status"] = "Read Error"
                    return file_info + (None,)

            except Exception:
                raise
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

                pp_cubes = iris.load(self.file_path)
                for cube in pp_cubes:
                    try:
                        n, s, e, w = self.getBoundingBox(cube)
                        start, end = self.getTemporalDomain(cube)

                        # Geospatial data
                        north.append(n)
                        south.append(s)
                        east.append(e)
                        west.append(w)

                        # Temporal data
                        start_time_list.append(start)
                        end_time_list.append(end)

                    except Exception as ex:
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
                        }
                    }

                phen_list = self.get_phenomena(pp_cubes)

                file_info[0]["info"]["read_status"] = "Successful"

                return file_info + phen_list + (spatial,)

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

    # file = "/badc/amma/data/ukmo-nrt/africa-lam/pressure_level_split/af/fp/2006/07/02/affp2006070218_05201_33.pp"
    file ="/Users/vdn73631/Documents/dev/ceda-fbs/python/src/test/files/affp2006070218_05201_33.pp"
    ppf = PpFile(file, level)
    start = datetime.datetime.today()
    print(ppf.get_metadata())
    end = datetime.datetime.today()
    print(end - start)
