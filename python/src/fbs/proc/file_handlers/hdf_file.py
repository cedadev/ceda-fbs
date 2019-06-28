'''
Created on 3 Jun 2016

@author: kleanthis
'''
from fbs.proc.file_handlers.generic_file import GenericFile

from pyhdf.HDF import HDF
from pyhdf.error import HDF4Error
from pyhdf.SD import SD, SDC
import six
import datetime

# Need to explicitly import these packages even though they are not directly used as per the note in HDF.py.
# This allows the vstart and vgstart methods to work.
# https://github.com/hdfeos/pyhdf/blob/master/pyhdf/HDF.py
from pyhdf import VS, V


class HdfFile(GenericFile):

    def __init__(self, file_path, level, **kwargs):
        GenericFile.__init__(self, file_path, level, **kwargs)
        self.handler_id = "hdf2."
        self.FILE_FORMAT = "hdf2."

    def _get_coords(self, vs):
        """
        Iterate through vgroup and return a list of coordinates (if existing).
        :param HDF4.V.vs vs: VData object
        :returns: Dict containing geospatial information.
        """
        mappings = {
            "NVlat2": "Latitude",
            "NVlng2": "Longitude",
        }

        coords = {}
        for k, v in six.iteritems(mappings):
            ref = vs.find(k)
            vd = vs.attach(ref)

            coords[v] = []
            while True:
                try:
                    coord = float(vd.read()[0][0])
                    coord /= 10 ** 7
                    coords[v].append(coord)
                except HDF4Error:  # End of file
                    break

            vd.detach()
        return coords

    def _get_temporal(self, vs):
        """
        Return start and end timestamps (if existing)
        :param HDF4.V.vs vs: VData object
        :param str fn: Path to the data file
        :returns: Dict containing temporal information.
        """
        mappings = {
            "MIdate": "date",
            "MIstime": "start_time",
            "MIetime": "end_time",
        }

        timestamps = {}
        for k, v in six.iteritems(mappings):
            ref = vs.find(k)
            vd = vs.attach(ref)

            timestamps[v] = []
            while True:
                try:
                    timestamps[v].append(vd.read()[0][0])
                except HDF4Error:  # EOF
                    break

            vd.detach()

        # This list comprehension basically converts from a list of integers
        # into a list of chars and joins them together to make strings
        # ...
        # If unclear - HDF text data comes out as a list of integers, e.g.:
        # 72 101 108 108 111 32 119 111 114 108 100 (this means "Hello world")
        # Those "char" numbers get converted to strings with this snippet.
        dates = [chr(x) for x in timestamps["date"] if x != 0]
        timestamps["date"] = ''.join(dates)

        return self._parse_timestamps(timestamps)

    @staticmethod
    def _parse_timestamps(tm_dict):
        """
        Parse start and end timestamps from an HDF4 file.
        :param dict tm_dict: The timestamp to be parsed
        :returns: Dict containing start and end timestamps
        """
        st_base = ("%s %s" % (tm_dict["date"], tm_dict["start_time"][0]))
        et_base = ("%s %s" % (tm_dict["date"], tm_dict["end_time"][0]))

        for t_format in ["%d/%m/%y %H%M%S", "%d/%m/%Y %H%M%S"]:
            try:
                start_time = datetime.datetime.strptime(st_base, t_format)
                end_time = datetime.datetime.strptime(et_base, t_format)
            except ValueError:
                # ValueError will be raised if strptime format doesn't match
                # the actual timestamp - so just try the next strptime format
                continue

        return {"start_time": start_time.isoformat(),
                "end_time": end_time.isoformat()}

    def _get_geolocation(self):
        # Open file.

        hdf = SD(self.file_path, SDC.READ)

        # Read geolocation dataset.
        try:
            lat = hdf.select('Latitude')
            latitude = lat[:, :].flatten()
            lon = hdf.select('Longitude')
            longitude = lon[:, :].flatten()
            return (latitude, longitude)
        except HDF4Error:
            return None

    @staticmethod
    def normalize_coord(coord):
        if coord < -180:
            coord = 0

        return coord

    def get_geospatial(self, v, vs):
        """
        Search through HDF4 file, returning a list of coordinates from the
        'Navigation' vgroup (if it exists).
        :returns: Dict containing geospatial information.
        """
        ref = -1

        while True:
            try:
                ref = v.getid(ref)
                vg = v.attach(ref)

                if vg._name == "Navigation":
                    geospatial = self._get_coords(vs)
                    geospatial["type"] = "track"  # Type annotation
                    vg.detach()
                    break

                vg.detach()
            except HDF4Error:  # End of file
                # This is a weird way of handling files, but this is what the
                # pyhdf library demonstrates...
                geospatial = None
                break

        if geospatial is not None:
            lat_u = self.normalize_coord(float(max(geospatial["Latitude"])))
            lat_l = self.normalize_coord(float(min(geospatial["Latitude"])))

            lon_u = self.normalize_coord(float(max(geospatial["Longitude"])))
            lon_l = self.normalize_coord(float(min(geospatial["Longitude"])))

            return {"coordinates": {"type": "envelope", "coordinates": [[round(lon_l, 3), round(lat_l, 3)],
                                                                        [round(lon_u, 3), round(lat_u, 3)]]}}

        # Second method.
        geospatial = self._get_geolocation()

        if geospatial is not None:
            lat_u = self.normalize_coord(float(max(geospatial[0])))
            lat_l = self.normalize_coord(float(min(geospatial[0])))

            lon_u = self.normalize_coord(float(max(geospatial[1])))
            lon_l = self.normalize_coord(float(min(geospatial[1])))

            return {"coordinates": {"type": "envelope", "coordinates": [[round(lon_l, 3), round(lat_l, 3)],
                                                                        [round(lon_u, 3), round(lat_u, 3)]]}}

    def get_temporal(self, v, vs):
        """
        Search through HDF4 file, returning timestamps from the 'Mission'
        vgroup (if it exists)
        :returns: List containing temporal metadata
        """
        ref = -1
        while True:
            try:
                ref = v.getid(ref)
                vg = v.attach(ref)

                if vg._name == "Mission":
                    temporal = self._get_temporal(vs)
                    vg.detach()
                    return temporal

                vg.detach()
            except HDF4Error:  # End of file
                # This 'except at end of file' thing is some pyhdf weirdness
                # Check the pyhdf documentation for clarification
                break

        return None

    def get_metadata_level3(self):
        self.handler_id = "Hdf handler level 3."

        file_info = self.get_metadata_level1()

        # First method for extracting information.
        try:
            self.hdf = HDF(self.file_path)
            file_info[0]["info"]["read_status"] = "Successful"
        except Exception:
            file_info[0]["info"]["read_status"] = "Read Error"
            return file_info

        vs = self.hdf.vstart()
        v = self.hdf.vgstart()

        spatial = self.get_geospatial(v, vs)
        temporal = self.get_temporal(v, vs)

        if temporal is not None:
            file_info[0]["info"]["temporal"] = temporal

        return file_info + (None, spatial)

    def get_metadata(self):

        if self.level == "1":
            res = self.get_metadata_level1()
        elif self.level == "2":
            res = self.get_metadata_level1()
        elif self.level == "3":
            res = self.get_metadata_level3()

        res[0]["info"]["format"] = self.FILE_FORMAT

        return res

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


if __name__ == "__main__":
    import sys

    # run test
    try:
        level = str(sys.argv[1])
        file = sys.argv[2]
    except IndexError:
        level = '1'
        file = '/neodc/arsf/2006/GB05_01/GB05_01-2006_208_Inveresk/L1b/c208031b.hdf'

    hdf = HdfFile(file, level)
    start = datetime.datetime.today()
    print(hdf.get_metadata())
    end = datetime.datetime.today()
    print(end - start)
