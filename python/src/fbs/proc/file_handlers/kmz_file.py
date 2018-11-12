'''
Created on 31 May 2016

@author: kleanthis
'''
from proc.file_handlers.generic_file import GenericFile
import zipfile
import xmltodict
import os
import re
import datetime
import dateutil.parser

class Coordinates:
    """
    Coordinates parser
    """

    def __init__(self, coordinate_string):
        self.coordinate_string = coordinate_string

    def bbx_coordinates(self):
        """
        Parse string in format "lon,lat lon,lat"
        """
        space_split = self.coordinate_string.split(" ")
        self.lon_l = float(space_split[0].split(",")[0])
        self.lat_l = float(space_split[0].split(",")[1])
        self.lon_u = float(space_split[1].split(",")[0])
        self.lat_u = float(space_split[1].split(",")[1])

    def point_coordinates(self):
        """
        Parse point string in format "lon,lat,---"
        :return:
        """
        self.point_lon = float(self.coordinate_string.split(',')[0])
        self.point_lat = float(self.coordinate_string.split(',')[1])


class KmzFile(GenericFile):
    """
    Class for returning basic information about the content
    of an nasaames file.
    """

    def __init__(self, file_path, level, additional_param=None, **kwargs):
        GenericFile.__init__(self, file_path, level, **kwargs)
        self.handler_id = "kmz file handler."
        self.FILE_FORMAT = "KMZ"

    def get_handler_id(self):
        return self.handler_id

    def find_elements(self, node, item_list, result):
        for key, item in node.items():
            if isinstance(item, dict):
                self.find_elements(item, item_list, result)
            else:
                if isinstance(item, list):
                    for sub_item in item:
                        if isinstance(sub_item, dict):
                            self.find_elements(sub_item, item_list, result)
                        else:
                            # print key, " : ", item
                            if key in item_list:
                                result.append(item)
                else:
                    # print key, " : ", item
                    if key in item_list:
                        result.append(key)
                        result.append(item)

    def doc_kml_not_basename(self, xml_doc):
        """
        Used to extract level 3 metadata from KMZ file where the uncompressed file within the kmz is called
        doc.kml not the same name as the KMZ file.
        :param xml_doc: An xml_file

        :return: Tuple containing dicts for the spatial and temporal information
        """
        print "doc.kml not <os.path.basename>.kml"

        xml_dict = xmltodict.parse(xml_doc)

        geo = xml_dict["kml"]["GroundOverlay"]["LatLonBox"]
        time = xml_dict["kml"]["GroundOverlay"]["name"]

        datestring = datetime.datetime.strptime(time[-13:], '%Y%m%d_%H%M').isoformat()

        # Load coordinates into list to get max, min to make sure coordinates are the right way round.
        lats = [float(geo["north"]),float(geo["south"])]
        lons = [float(geo["east"]),float(geo["west"])]

        max_lat = max(lats)
        min_lat = min(lats)
        max_lon = max(lons)
        min_lon = min(lons)

        bbox = {'coordinates': {'type': 'envelope', 'coordinates': [[round(min_lon, 3), round(min_lat, 3)],
                                                                    [round(max_lon, 3), round(max_lat, 3)]]}}
        temporal = {'start_time': datestring, 'end_time': datestring}

        return bbox, temporal

    def kml_document(self, xml_dict):
        """
        Extract level 3 metadata when structure is xml_dict["kml"]["Document"]
        :param xml_dict: xml doc converted to dict

        :return: Tuple containing dicts for the spatial and temporal information
        """


        print "kml_document"

        lats = []
        lons = []

        date_str =''

        datetime_strs = []

        placemark_root = xml_dict["kml"]["Document"]["Folder"][1]["Placemark"]

        for i, placemark in enumerate(placemark_root):
            coordinates = Coordinates(placemark["LineString"]["coordinates"])

            if len(placemark["LineString"]["coordinates"].split()) == 2:
                coordinates.bbx_coordinates()

                lons.append(coordinates.lon_l)
                lats.append(coordinates.lat_l)
                lons.append(coordinates.lon_u)
                lats.append(coordinates.lat_u)
            else:
                coordinates.point_coordinates()

                lons.append(coordinates.point_lon)
                lats.append(coordinates.point_lat)

            date = placemark["description"]["table"]["tr"][1]["td"][1]
            time = placemark["description"]["table"]["tr"][2]["td"][1]

            # Handle situations where the date is formatted dd-mm-yyyy
            if len(date.split('-')[0]) == 4:
                date_str = '{}T{}'.format(date,time)
            elif len(date.split('-')[0]) == 2:
                date_str = dateutil.parser.parse('{}T{}'.format(date,time), dayfirst=True).isoformat()

            datetime_strs.append(date_str)

        min_lat = min(lats)
        min_lon = min(lons)
        max_lat = max(lats)
        max_lon = max(lons)

        start_date = min(datetime_strs)
        end_date = max(datetime_strs)

        bbox = {'coordinates': {'type': 'envelope', 'coordinates': [[round(float(min_lon), 3), round(float(min_lat), 3)],
                                                                    [round(float(max_lon), 3), round(float(max_lat), 3)]]}}
        temporal = {'start_time': start_date, 'end_time': end_date}

        return bbox, temporal

    def kml_folder(self, xml_dict):
        """
        Process file which has the structure kml_dict["kml"]["Folder"]
        :param xml_dict: xml doc converted to dict

        :return: Tuple containing dicts for the spatial and temporal information
        """
        print "kml_folder"

        # Date and time regex patterns
        date_regex = re.compile(r'([\d]{8})')
        time_regex = re.compile(r'([\d]{2}:[\d]{2}:[\d]{2})')

        # Set up arrays
        times = []
        lons = []
        lats = []

        # Extract date
        date_str = date_regex.search(xml_dict["kml"]["Folder"]["name"]).group(1)

        # Location for placemark
        placemark_root = xml_dict["kml"]["Folder"]["Folder"]["Placemark"]

        for i, placemark in enumerate(placemark_root):
            # Extract time
            times.append(time_regex.search(placemark["name"]).group(1))

            # Parse coordinates string
            coordinates = Coordinates(placemark["Point"]["coordinates"])
            coordinates.point_coordinates()

            # Append coordinates to list
            lons.append(coordinates.point_lon)
            lats.append(coordinates.point_lat)

        # Get bounding box coordinates
        c1 = min(lats)
        c2 = min(lons)
        c3 = max(lats)
        c4 = max(lons)

        bbox = {'coordinates': {'type': 'envelope', 'coordinates': [[round(float(c2), 3), round(float(c1), 3)],
                                                                    [round(float(c4), 3), round(float(c3), 3)]]}}

        # Get date
        start_time = min(times)
        end_time = max(times)

        # Format dates
        start_date = datetime.datetime.strptime(date_str + start_time, '%Y%m%d%H:%M:%S').isoformat()
        end_date = datetime.datetime.strptime(date_str + end_time, '%Y%m%d%H:%M:%S').isoformat()

        temporal = {'start_time': start_date, 'end_time': end_date}

        return bbox, temporal

    def get_metadata_level3(self):
        self.handler_id = "kmz handler level 3."
        spatial = None

        file_info = self.get_metadata_level1()

        try:

            if self.file_path.endswith('.kmz'):
                archive = zipfile.ZipFile(self.file_path, 'r')
                uncompressed_file = os.path.basename(self.file_path)
                uncompressed_file = uncompressed_file.replace(".kmz", ".kml")

                try:
                    # Try and read the file using the file basename as the key.
                    # If this fails then it is possible that the uncompressed file is called doc.kml
                    xml_doc = archive.read(uncompressed_file)

                except KeyError:
                    # Try accessing the file with the kml document key of doc.kml
                    xml_doc = archive.read('doc.kml')
                    spatial, temporal = self.doc_kml_not_basename(xml_doc)

                    file_info[0]["info"]["temporal"] = temporal
                    file_info[0]["info"]["read_status"] = "Successful"

                    return file_info + (None, spatial,)

            else:
                with open(self.file_path, 'r') as xml_file:
                    xml_doc = xml_file.read()

            xml_dict = xmltodict.parse(xml_doc)

            # There are two possible routes to the metadata, ["kml"]["Document"] or ["kml"]["folder"]
            try:
                xml_dict["kml"]["Document"]
                spatial, temporal = self.kml_document(xml_dict)

                file_info[0]["info"]["temporal"] = temporal
                file_info[0]["info"]["read_status"] = "Successful"

                return file_info + (None, spatial,)

            except KeyError:
                spatial, temporal = self.kml_folder(xml_dict)

                file_info[0]["info"]["temporal"] = temporal
                file_info[0]["info"]["read_status"] = "Successful"

                return file_info + (None, spatial,)

        except Exception:

            file_info[0]["info"]["read_status"] = "Read Error"
            return file_info

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
    import datetime
    import sys

    # run test
    try:
        level = str(sys.argv[1])
    except IndexError:
        level = '1'

    # Has key xml_dict["kml"]["Document"],Coordinates are points, string indices must be integers.
    file = '/neodc/arsf/2011/GB11_05/GB11_05-2011_285_London/photography/GB11_05-2011_285.kml'

    # Has key xml_dict["kml"]["folder"]
    file='/badc/faam/data/2006/b237-aug-22/flight-track_faam_20060822_b237.kml'

    # Has key xml_dict["kml"]["Document"], coordinates are bbox
    # file = '/neodc/sister/data/Festival/KML/2006/Alice_GE_2006W28_Festival.kmz'

    # Uncompressed file not named same as file eg not O3_18_20110119_1800.kml
    # file = '/badc/ronoco/data/model-output-images/20110119/o3/kmz/O3_18_20110119_1800.kmz'

    # Test
    # file='/neodc/arsf/2014/GB12_03/GB12_03-2014_169_Loch_Lomond/photography/GB12_03-2014_169.kml'
    file='/neodc/sister/data/QM2/KML/2011/Alice_GE_2011W06_QM2.kmz'

    kmf = KmzFile(file, level)
    start = datetime.datetime.today()
    print kmf.get_metadata()
    end = datetime.datetime.today()
    print end - start
