'''
Created on 2 Jun 2016

@author: kleanthis
'''
from proc.file_handlers.generic_file import GenericFile
import proc.common_util.util as util
import csv
import re

class BadcCsvFile(GenericFile):

    def __init__(self, file_path, level, additional_param=None):
        GenericFile.__init__(self, file_path, level)
        self.handler_id = "Badc csv"
        self.FILE_FORMAT = self.get_file_format()

    def get_file_format(self):
        with open(self.file_path) as fp:
            if 'BADC-CSV' in fp.readline():
                return 'BADC CSV'
            else:
                return 'CSV'

    def get_handler_id(self):
        return self.handler_id

    def csv_parse(self, fp):

        phenomena = {}
        date = None
        location = None

        reader = csv.reader(fp)

        for row in reader:
            new_phenomenon = {}

            if row[0] == "data":
                break
            elif row[1] == "G":
                if row[0] == "date_valid":
                    date = row[2]
                if row[0] == "location":
                    location = row[2]
                continue
            else:
                if row[1] in phenomena:
                    phenomena[row[1]]["attributes"] .append({"name": row[0], "value": re.sub(r'[^\x00-\x7F]+',' ', row[2])})
                else:
                    new_phenomenon["attributes"] = []
                    new_phenomenon["attributes"].append({"name": row[0], "value": re.sub(r'[^\x00-\x7F]+',' ', row[2])})
                    phenomena[row[1]] = new_phenomenon

        return (phenomena, date, location)

    def get_phenomena(self, fp):

        phen_list = []

        phenomena, _, _ = self.csv_parse(fp)

        for key in phenomena.keys():
            phen_list.append(phenomena[key])

        file_phenomena = util.build_phenomena(phen_list)

        return file_phenomena

    def get_metadata_level2(self):
        self.handler_id = "Csv handler level 2."

        file_info = self.get_metadata_level1()

        if file_info is not None:
            with open(self.file_path) as fp:
                phen = self.get_phenomena(fp)

            file_info[0]["info"]["read_status"] = "Successful"
            return  file_info +  phen
        else:
            return None

    def get_metadata_level3(self):
        self.handler_id = "Csv handler level 3."

        loc = (None,)

        file_info = self.get_metadata_level1()

        if file_info is not None:
            with open(self.file_path) as fp:
                phen = self.csv_parse(fp)
                fp.seek(0)
                phenomena = self.get_phenomena(fp)

            if phen[1] is not None:
                file_info[0]["info"]["temporal"] = {"start_time": phen[1], "end_time": phen[1]}

            if phen[2] is not None:
                if phen[2] == 'global':
                    loc = ({'coordinates': {'type': 'envelope', 'coordinates': [[-180.0,90.0], [180.0, -90.0]]}},)

            return file_info + phenomena + loc
        else:
            return None


    def get_metadata(self):

        if self.FILE_FORMAT == 'CSV':
            res = self.get_metadata_level1()
        else:
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

    file = '/badc/ukmo-metdb/data/amdars/2016/12/ukmo-metdb_amdars_20161222.csv'
    baf = BadcCsvFile(file,level)
    start = datetime.datetime.today()
    print baf.get_metadata()
    end = datetime.datetime.today()
    print end-start