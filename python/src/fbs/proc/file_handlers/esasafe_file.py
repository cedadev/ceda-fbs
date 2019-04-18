'''
Created on 12 May 2016

@author: kleanthis
'''

from fbs.proc.file_handlers.generic_file import GenericFile
import fbs.proc.common_util.util as util
import xml.etree.cElementTree as ET

# Set up name spaces for use in XML paths 
ns = {
        "gml": "http://www.opengis.net/gml",
        "gx": "http://www.google.com/kml/ext/2.2",
        "s1": "http://www.esa.int/safe/sentinel-1.0/sentinel-1",    
        "s1sar": "http://www.esa.int/safe/sentinel-1.0/sentinel-1/sar",
        "s1sarl1": "http://www.esa.int/safe/sentinel-1.0/sentinel-1/sar/level-1",
        "s1sarl2": "http://www.esa.int/safe/sentinel-1.0/sentinel-1/sar/level-2",
        "safe": "http://www.esa.int/safe/sentinel-1.0",    
        "version": "esa/safe/sentinel-1.0/sentinel-1/sar/level-1/slc/standard/iwsp",
        "xfdu": "urn:ccsds:schema:xfdu:1",
    }

    # Define mappings dictionary of XML paths to sections we are capturing
mappings = {
        "platform": {
           "common_prefix": 
           "./metadataSection/metadataObject[@ID='platform']/metadataWrap/xmlData/",
        "properties": {
         "Platform Family Name": "{%(safe)s}platform/{%(safe)s}familyName" % ns, 
         "NSSDC Identifier": "{%(safe)s}platform/{%(safe)s}nssdcIdentifier" % ns,
         "Instrument Family Name": "{%(safe)s}platform/{%(safe)s}instrument/{%(safe)s}familyName" % ns,
         "Mode": "{%(safe)s}platform/{%(safe)s}instrument/{%(safe)s}extension/{%(s1sarl1)s}instrumentMode/{%(s1sarl1)s}mode" % ns,
        },
    },
    "spatial": {
       "common_prefix":
         "./metadataSection/metadataObject[@ID='measurementFrameSet']/metadataWrap/xmlData/",
       "transformers": {
         # Defines method name used to convert string of coordinates into list of tuples containing floats
         "Coordinates": "_package_coordinates",
       },
       "properties": {
         "Coordinates": "{%(safe)s}frameSet/{%(safe)s}frame/{%(safe)s}footPrint/{%(gml)s}coordinates" % ns,
       },
    },
    "product_info": {
       "common_prefix":
         "./metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/",
       "properties": {
         "Product Class": "{%(s1sarl1)s}standAloneProductInformation/{%(s1sarl1)s}productClass" % ns,
         "Product Class Description": "{%(s1sarl1)s}standAloneProductInformation/{%(s1sarl1)s}productClassDescription" % ns,
         "Timeliness Category": "{%(s1sarl1)s}standAloneProductInformation/{%(s1sarl1)s}productTimelinessCategory" % ns,
         "Product Composition": "{%(s1sarl1)s}standAloneProductInformation/{%(s1sarl1)s}productComposition" % ns,
         "Polarisation": "{%(s1sarl1)s}standAloneProductInformation/{%(s1sarl1)s}transmitterReceiverPolarisation" % ns,
       },
    },
    "orbit_info": {
       "common_prefix":
         "./metadataSection/metadataObject[@ID='measurementOrbitReference']/metadataWrap/xmlData/",
       "properties": {
         "Start Relative Orbit Number": "{%(safe)s}orbitReference/{%(safe)s}relativeOrbitNumber[@type='start']" % ns,
         "Stop Relative Orbit Number": "{%(safe)s}orbitReference/{%(safe)s}relativeOrbitNumber[@type='stop']" % ns,
         "Start Orbit Number": "{%(safe)s}orbitReference/{%(safe)s}orbitNumber[@type='start']" % ns,
         "Stop Orbit Number": "{%(safe)s}orbitReference/{%(safe)s}orbitNumber[@type='stop']" % ns,
         "Pass Direction": "{%(safe)s}orbitReference/{%(safe)s}extension/{%(s1)s}orbitProperties/{%(s1)s}pass" % ns,
         "Phase Identifier": "{%(safe)s}orbitReference/{%(safe)s}phaseIdentifier" % ns,
         "Cycle Number": "{%(safe)s}orbitReference/{%(safe)s}cycleNumber" % ns,
       },
    },
    "acquisition_period": {
       "common_prefix":
         "./metadataSection/metadataObject[@ID='acquisitionPeriod']/metadataWrap/xmlData/",
       "properties": {
         "Start Time": "{%(safe)s}acquisitionPeriod/{%(safe)s}startTime" % ns,
         "Stop Time": "{%(safe)s}acquisitionPeriod/{%(safe)s}stopTime" % ns,
       }
    }
}

class EsaSafeFile(GenericFile):


    """
    Class for returning basic information about the content
    of an manifest file.
    """

    def __init__(self, file_path, level, additional_param=None, **kwargs):
        GenericFile.__init__(self, file_path, level, **kwargs)
        self.handler_id = "Manifest handler level 3."
        self.FILE_FORMAT = "Manifest"

    def get_handler_id(self):
        return self.handler_id

    def _open_file(self):
        self.root = ET.parse(self.file_path).getroot()
        self._parse_content()


    def _parse_content(self):
        self.sections = {}

        for section_id, content_dict in mappings.items():
            self.sections[section_id] = {}
            prefix = content_dict["common_prefix"]

            for item_name, xml_path_end in content_dict["properties"].items():
                xml_path = prefix + xml_path_end

                try:
                    value = self.root.find(xml_path).text
                    if item_name in content_dict.get("transformers", {}):
                        transformer = getattr(self, content_dict["transformers"][item_name])
                        value = transformer(value)

                    self.sections[section_id][item_name] = value
                except:
                    print "FAILED: %s  -->  %s" % (section_id, xml_path)


    def _package_coordinates(self, coords_string):
        """
        Converts a coordinates string into a dictionary of lats and lons.
        :param string coords_string: a string of lat,lon pairs separated by commas
        :returns: Dictionary of: {"lats": <list of lats>, "lons": <list of lons>}
        """
        coord_pairs = coords_string.split()
        lats = []
        lons = []

        for ll_string in coord_pairs:
            lat, lon = ll_string.split(",")
            lats.append(float(lat))
            lons.append(float(lon))

        return {"lat": lats, "lon": lons, "type": "swath"}

    def get_geospatial(self):
        """
        Return coordinates.
        :returns: Dict containing geospatial information.
        """
        return self.sections["spatial"]["Coordinates"]

    def get_temporal(self):
        """
        Returns temporal window.

        :returns: List containing temporal metadata
        """
        ap = self.sections["acquisition_period"]
        iso_start_date = util.date2iso(ap["Start Time"])
        iso_end_date = util.date2iso(ap["Stop Time"])

        return {"start_time": iso_start_date,
                "end_time": iso_end_date }

    def get_metadata_level3(self):
        self.handler_id = "Manifest handler level 3."

        res = self.get_metadata_level1()

        try:
            self._open_file()

        except Exception:
            # Error reading file
            res[0]["info"]["read_status"] = "Read Error"
            return res

        # File read successful
        geospatial = self.get_geospatial()
        temporal = self.get_temporal()

        lat_u =  max(geospatial["lat"])
        lat_l =  min(geospatial["lat"])

        lon_u = max(geospatial["lon"])
        lon_l =  min(geospatial["lon"])

        spatial = {"coordinates": {"type": "envelope", "coordinates": [[round(lon_l, 3), round(lat_l, 3)], [round(lon_u, 3), round(lat_u, 3)]] } }
        res[0]["info"]["temporal"] = {"start_time": temporal["start_time"], "end_time": temporal["end_time"] }
        res[0]["info"]["read_status"] = "Successful"

        phenomena = None

        return res + (phenomena, spatial,)

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

    file = '/neodc/sentinel1a/data/IW/L1_GRD/h/IPF_v2/2017/10/31/S1A_IW_GRDH_1SDV_20171031T061411_20171031T061436_019053_020395_DCCA.manifest'
    esf = EsaSafeFile(file,level)
    start = datetime.datetime.today()
    print esf.get_metadata()
    end = datetime.datetime.today()
    print end-start