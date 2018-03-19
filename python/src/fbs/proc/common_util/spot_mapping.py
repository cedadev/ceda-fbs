import requests
import os

class SpotMapping(object):
    """
    Downloads the spot mapping from the cedaarchiveapp.
    Makes two queryable dicts:
        spot2pathmapping = provide spot and return file path
        path2spotmapping = provide a file path and the spot will be returned
    """
    url = "http://cedaarchiveapp.ceda.ac.uk/cedaarchiveapp/fileset/download_conf/"
    spot2pathmapping = {}
    path2spotmapping = {}

    def __init__(self, test=False, from_file=False, spot_file=None):

        if test:
            self.spot2pathmapping['spot-1400-accacia'] = "/badc/accacia"
            self.spot2pathmapping['abacus'] = "/badc/abacus"

        elif from_file:
            with open(spot_file) as reader:
                lines = reader.readlines()
                for line in lines:
                    spot, path = line.strip().split('=')
                    self.spot2pathmapping[spot] = path
                    self.path2spotmapping[path] = spot

        else:
            response = requests.get(self.url)
            log_mapping = response.text.split('\n')

            for line in log_mapping:
                if not line.strip(): continue
                print line.strip().split()
                spot, path = line.strip().split()
                if spot in ("spot-2502-backup-test",): continue
                self.spot2pathmapping[spot] = path
                self.path2spotmapping[path] = spot

    def __iter__(self):
        return iter(self.spot2pathmapping)

    def __len__(self):
        return len(self.spot2pathmapping)

    def get_archive_root(self, key):
        """

        :param key: Provide the spot
        :return: Returns the directory mapped to that spot
        """
        return self.spot2pathmapping[key]

    def get_spot(self, key):
        """
        The directory stored in elasticsearch is the basename for the specific file. The directory stored on the spots
        page is further up the directory structure but there is no common cut off point as it depends on how many files there
        are in each dataset. This function recursively starts at the end of the directory stored in elasticsearch
        and gradually moves back up the file structure until it finds a match in the path2spot dict.

        :param key: Provide a filename or directory
        :return: Returns the spot which encompasses that file or directory.
        """

        while (key not in self.path2spotmapping) and (key != '/'):
            key = os.path.dirname(key)

        if key == '/':
            return None

        return self.path2spotmapping[key]