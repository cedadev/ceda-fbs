import os
import fbs.proc.common_util.util as util


class GenericFile(object):
    """
    Class for returning basic information about a file.
    """

    LEVEL_MAP = {
        "1": 'get_metadata_level1',
        "2": 'get_metadata_level2',
        "3": 'get_metadata_level3',
    }

    def __init__(self, file_path, level, calculate_md5=False):
        self.file_path = file_path
        self.level = str(level)
        self.handler_id = None
        self.calculate_md5 = calculate_md5

    def _get_file_ownership(self):

        uid = os.stat(self.file_path).st_uid
        gid = os.stat(self.file_path).st_gid

        return uid, gid

    def get_metadata_level1(self):
        """
        Scans the given file and returns information about 
        the file not the content.
        :returns: A dict containing summary information.
        """

        self.handler_id = "Generic level 1."

        #Do the basic checking, if file exists 
        #and that it is not a symbolic link.
        if ( self.file_path is None
             or not os.path.isfile(self.file_path)
             # or os.path.islink(self.file_path)
           ):
            return None

        file_info = {}
        info = {}

        file_stats = os.stat(self.file_path)

        #Basic information. 
        info["name"] = os.path.basename(self.file_path) #ntpath.basename(file_path)
        info["name_auto"] = info["name"]
        info["directory"] = os.path.dirname(self.file_path)
        info["location"] = "on_disk"

        uid,gid = self._get_file_ownership()
        info["user"] = uid
        info["group"] = gid

        info["is_link"] = os.path.islink(self.file_path)

        info["last_modified"] = datetime.datetime.fromtimestamp(file_stats.st_mtime).isoformat()

        info["size"] = os.path.getsize(self.file_path)

        file_type = os.path.splitext(info["name"])[1]
        if len(file_type) == 0:
            file_type = "File without extension."

        info["type"] = file_type

        if self.calculate_md5:
            info["md5"] = util.calculate_md5(self.file_path)

        if getattr(self, 'FILE_FORMAT', None):
            info["format"] = self.FILE_FORMAT

        file_info["info"] = info
        return (file_info, )

    def get_metadata_level2(self):

        """
         Wrapper for method get_properties_generic_level1().
        :returns: A dict containing information compatible with current es ops.
        """

        file_info = self.get_metadata_level1()

        self.handler_id = "Generic level 2."

        if file_info is None:
            return None

        return file_info

    def get_metadata_level3(self):
        file_info = self.get_metadata_level2()
        self.handler_id = "Generic level 3."
        return file_info

    def get_metadata(self):

        metadata_function = self.LEVEL_MAP.get(self.level)

        if getattr(self, metadata_function, None):
            return getattr(self, metadata_function)()

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

    gf = GenericFile(file, level)
    start = datetime.datetime.today()
    print(gf.get_metadata())
    end = datetime.datetime.today()
    print(end - start)
