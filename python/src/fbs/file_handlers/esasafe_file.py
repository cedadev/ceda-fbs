'''
Created on 12 May 2016

@author: kleanthis
'''

from fbs.file_handlers.generic_file import GenericFile
import fbs_lib.util as util
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

    def __init__(self, file_path, level, additional_param=None):
        GenericFile.__init__(self, file_path, level)
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
        return {"start_time": ap["Start Time"],
                "end_time": ap["Stop Time"]}


    def get_metadata_esasafe_level3(self):
        self.handler_id = "MAnifest handler level 3."
        spatial = None

        res = self.get_metadata_generic_level1()

        self._open_file()
        geospatial = self.get_geospatial()
        temporal = self.get_temporal()

        #{'lat': [-56.301735, -54.830536, -58.449139, -60.058411], 'type': 'swath', 'lon': [-37.49408, -31.223965, -28.045124, -34.86174]}
        #{'start_time': '2015-07-04T21:33:30.724498', 'end_time': '2015-07-04T21:34:36.080966'}

        lat_u =  max(geospatial["lat"])
        lat_l =  min(geospatial["lat"])

        lon_u = max(geospatial["lon"])
        lon_l =  min(geospatial["lon"])


        spatial  =  {"coordinates": {"type": "envelope", "coordinates": [[lon_l, lat_l], [lon_u, lat_u]] } }
        res[0]["info"]["temporal"] = {"start_time": temporal["start_time"], "end_time": temporal["end_time"] }

        phenomena = None

        return res + (phenomena, spatial,)

    def get_metadata(self):

        if self.level == "1":
            res = self.get_metadata_generic_level1()
        elif self.level == "2":
            res = self.get_metadata_generic_level1()
        elif self.level == "3":
            res = self.get_metadata_esasafe_level3()

        res[0]["info"]["format"] = self.FILE_FORMAT

        return res

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass
