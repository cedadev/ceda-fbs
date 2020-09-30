import os
import re

from fbs.proc.common_util.util import FileFormatError
from fbs.proc.file_handlers import generic_file
from fbs.proc.file_handlers import netcdf_file
from fbs.proc.file_handlers import nasaames_file
from fbs.proc.file_handlers import pp_file
from fbs.proc.file_handlers import grib_file
from fbs.proc.file_handlers import esasafe_file
from fbs.proc.file_handlers import kmz_file
from fbs.proc.file_handlers import hdf_file
from fbs.proc.file_handlers import badc_csv_file
from fbs.proc.file_handlers import metadata_tags_json_file

import magic as magic_number_reader
import fbs.proc.common_util.util as util
import six


class HandlerPicker(object):
    """
    Returns a file handler for the supplied file.
    """

    HANDLER_MAP = {
        '.nc': netcdf_file.NetCdfFile,
        '.na': nasaames_file.NasaAmesFile,
        '.pp': pp_file.PpFile,
        '.grb':  grib_file.GribFile,
        '.grib': grib_file.GribFile,
        '.manifest': esasafe_file.EsaSafeFile,
        '.kmz': kmz_file.KmzFile,
        '.hdf': hdf_file.HdfFile
    }

    def __init__(self):
        self.handlers_and_dirs = {}
        self.NETCDF_PYTHON_MAGIC_NUM_RES = "NetCDF Data Format data"
        self.ASCII_PYTHON_MAGIC_NUM_RES = "ASCII text"
        self.DATA_PYTHON_MAGIC_NUM_RES = "data"

    def pick_best_handler(self, filename):
        """
        :param filename : the file to be scanned.
        :returns handler: Returns an appropriate handler
        for the given file.
        """

        file_dir = os.path.dirname(filename)
        file_basename = os.path.basename(filename)

        if file_basename == "metadata_tags.json":
            handler = metadata_tags_json_file.MetadataTagsJsonFile

        else:
            # Try returning a handler based on file extension.
            extension = os.path.splitext(filename)[1]
            extension = extension.lower()

            if extension == '.csv':
                try:
                    header = util.get_bytes_from_file(filename, 500)
                    pattern_to_search = "Conventions,G,BADC-CSV"
                    res = header.find(pattern_to_search)

                    if res != -1:
                        handler = badc_csv_file.BadcCsvFile
                    else:
                        handler = generic_file.GenericFile

                except Exception:  # catch everything... if there is an error just return the generic handler.
                    handler = generic_file.GenericFile

            else:
                handler = self.HANDLER_MAP.get(extension, generic_file.GenericFile)

        if handler is not None:
            self.handlers_and_dirs[file_dir] = handler
            return handler

        # Try returning a handler based on file's magic number.
        try:
            res = magic_number_reader.from_file(filename)

            if res == self.NETCDF_PYTHON_MAGIC_NUM_RES:
                handler = netcdf_file.NetCdfFile

            elif res == self.ASCII_PYTHON_MAGIC_NUM_RES:
                # ok lets see if it is a na file.
                first_line = util.get_file_header(filename)
                tokens = first_line.split(" ")

                if len(tokens) >= 2:
                    if tokens[0].isdigit() and tokens[1].isdigit():
                        handler = nasaames_file.NasaAmesFile
                else:
                    handler = generic_file.GenericFile

            # This can be a grb file.
            elif res == self.DATA_PYTHON_MAGIC_NUM_RES:
                res = util.get_bytes_from_file(filename, 4)

                if res == "GRIB":
                    handler = grib_file.GribFile
                else:
                    handler = generic_file.GenericFile

        except Exception:  # catch everything... if there is an error just return the generic handler.
            handler = generic_file.GenericFile

        if handler is not None:
            self.handlers_and_dirs[file_dir] = handler
            return handler

        # Try to return last handler used in this directory.
        if file_dir in self.handlers_and_dirs.keys():
            handler = self.handlers_and_dirs[file_dir]

        if handler is not None:
            return handler

        # Nothing worked, return the generic handler.
        handler = generic_file.GenericFile

        return handler

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass
