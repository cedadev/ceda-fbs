"""
Interface to extract and generate JSON from HDF4 EUFAR metadata
"""

import datetime

from pyhdf.HDF import HDF
from pyhdf.VS import VS
from pyhdf.V import V
from pyhdf.error import HDF4Error

from _dataset import _geospatial
from metadata import product


class HDF4(_geospatial):
    """
    ARSF/EUFAR metadata HDF4 context manager class.
    """
    hdf = None
    vs = None
    v = None

    def __init__(self, fname):
        """
        :param str fname: The path of the HDF4 file.
        """
        self.fname = fname

    def __enter__(self):
        """
        Open HDF file and interfaces for use as context manager.
        :return self:
        """
        self.hdf = HDF(self.fname)
        self.vs = self.hdf.vstart()
        self.v = self.hdf.vgstart()

        return self

    def __exit__(self, *args):
        """
        Close interfaces and HDF file after finishing use in context manager.
        """
        self.v.end()
        self.vs.end()
        self.hdf.close()

    def _get_coords(self, v, vs, fn):
        """
        Iterate through vgroup and return a list of coordinates (if existing).

        :param HDF4.V.v v: VGroup object
        :param HDF4.V.vs vs: VData object
        :param str fn: Filename of the object
        :return dict: Dict containing geospatial and temporal information.
        """
        mappings = {
            "NVlat2": "lat",
            "NVlng2": "lon",
        }

        coords = {}
        for k, v in mappings.iteritems():
            ref = vs.find(k)
            vd = vs.attach(ref)

            coords[v] = []
            while True:
                try:
                    coord = float(vd.read()[0][0]) / (10**7)
                    coords[v].append(coord)
                except HDF4Error:  # End of file
                    break

            vd.detach()
        return coords

    def _get_temporal(self, v, vs, fn):
        """
        Returns start and end timestamps (if existing)
        """
        mappings = {
            "MIdate": "date",
            "MIstime": "start_time",
            "MIetime": "end_time",
        }

        timestamps = {}
        for k, v in mappings.iteritems():
            ref = vs.find(k)
            vd = vs.attach(ref)

            timestamps[v] = []
            while True:
                try:
                    timestamps[v].append(vd.read()[0][0])
                except HDF4Error:  # EOF
                    break

            vd.detach()

        # Convert date from list of integers to string (because HDF is weird)
        timestamps["date"] = ''.join([chr(x) for x in timestamps["date"]
                                      if x != 0])
        return timestamps

    def _parse_timestamp(self, tm_dict):
        """
        Parse start and end timestamps from an HDF4 file.

        :param dict tm_dict: The timestamp to be parsed
        """
        st_base = ("%s %s", tm_dict["date"], tm_dict["start_time"])
        et_base = ("%s %s", tm_dict["date"], tm_dict["end_time"])

        start_time = datetime.datetime.strptime(st_base, "%d/%m/%Y %H%M%S")
        end_time = datetime.datetime.strptime(et_base, "%d/%m/%Y %H%M%S")
        return {"start_time": start_time,
                "end_time": end_time}

    def get_geospatial(self):
        """
        Search through HDF4 file, returning a list of coordinates from the
        'Navigation' vgroup (if it exists).

        :return dict: Dict containing geospatial information.
        """
        ref = -1
        while True:
            try:
                ref = self.v.getid(ref)
                vg = self.v.attach(ref)

                if vg._name == "Navigation":
                    geospatial = self._get_coords(self.v, self.vs, self.fname)
                    vg.detach()
                    return geospatial

                vg.detach()
            except HDF4Error:  # End of file
                # This is a weird way of handling files, but this is what the
                # pyhdf library demonstrates...
                break

        return None

    def get_temporal(self):
        """
        Search through HDF4 file, returning timestamps from the 'Mission'
        vgroup (if it exists)
        """
        ref = -1
        while True:
            try:
                ref = self.v.getid(ref)
                vg = self.v.attach(ref)

                if vg._name == "Mission":
                    temporal = self._get_temporal(self.v, self.vs, self.fname)
                    vg.detach()
                    return temporal

                vg.detach()
            except HDF4Error:  # End of file
                # This 'except at end of file' thing is some pyhdf weirdness
                # Check the pyhdf documentation for clarification
                break

        return None

    def get_properties(self):
        """
        :return eufar.metadata.properties.Properties: Metadata object
        Returns eufar.metadata.properties.Properties object
        containing geospatial and temporal metadata from file.
        """
        geospatial = self.get_geospatial()
        temporal = self.get_temporal()
        file_level = super(HDF4, self).get_file_level(self.fname)
        data_format = {
            "format": "HDF4",
        }

        props = product.Properties(spatial=geospatial,
                                   temporal=temporal,
                                   file_level=file_level,
                                   data_format=data_format)

        return props
