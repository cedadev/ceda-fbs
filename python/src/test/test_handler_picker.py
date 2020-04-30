# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '30 Apr 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

import unittest
from fbs.proc.file_handlers.handler_picker import HandlerPicker
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


class TestHandlerPicker(unittest.TestCase):
    HANDLER_MAP = {
        'netcdf': netcdf_file.NetCdfFile,
        'nasaames': nasaames_file.NasaAmesFile,
        'pp': pp_file.PpFile,
        'grib': grib_file.GribFile,
        'manifest': esasafe_file.EsaSafeFile,
        'kmz': kmz_file.KmzFile,
        'hdf': hdf_file.HdfFile,
        'badccsv': badc_csv_file.BadcCsvFile,
        'generic': generic_file.GenericFile,
        'metadata': metadata_tags_json_file.MetadataTagsJsonFile
    }

    @classmethod
    def setUpClass(cls):
        cls.handler_picker = HandlerPicker()

    def run_test(self, filenames, expected):
        for file in filenames:
            handler = self.handler_picker.pick_best_handler(file)
            self.assertEqual(handler, self.HANDLER_MAP[expected])

    def test_netCDF(self):
        filename = '/neodc/esacci/sst/data/gmpe/lt/1992/11/21/19921121120000-UKMO-L4_GHRSST-SST-GMPEREAN-GLOB-v02.0-fv02.0.nc'
        self.run_test([filename], 'netcdf')

    def test_README(self):
        filename = '/neodc/esacci/sst/00README'
        self.run_test([filename], 'generic')

    def test_text_file(self):
        filename = '/neodc/esacci/sst/esacci_sst_terms_and_conditions.txt'
        self.run_test([filename], 'generic')

    def test_nasaAMES(self):
        filename = '/badc/amazonica/data/greenhousegases/nasa_ames/amazonica_chemistry_rba_20100108.na"'
        self.run_test([filename], 'nasaames')

    def test_pp_file(self):
        filename = '/badc/amma/data/ukmo-nrt/africa-lam/pressure_level_split/af/fp/2006/10/31/affp2006103118_00023_15.pp'
        self.run_test([filename], 'pp')

    def test_grib_file(self):
        filenames = [
            '/badc/ecmwf-for/troccinox/data/2008/03/30/model/FC.w.2008033012.120.grib',
            '/badc/ecmwf-op/.op_cache_gp/gp/am/2002/09/21/gpam2002092100v.grb',
        ]
        self.run_test(filenames, 'grib')

    def test_manifest_file(self):
        filename = '/neodc/sentinel1a/data/IW/L1_SLC/IPF_v2/2015/03/04/S1A_IW_SLC__1SSV_20150304T104541_20150304T104609_004881_006154_AD33.manifest'
        self.run_test([filename], 'manifest')

    def test_kmz_file(self):
        filename = '/badc/faam/doc/FAAM_Website_2004-2010/Website/public/campaigns/2008-9winter/b425satmap.kmz'
        self.run_test([filename], 'kmz')

    def hdf_file(self):
        filename = '/badc/cloudmap/data/ral/atsr_cloud-products/1997/03/version2/ATSR2-RAL-V002_19970327-1412_28n053w-34n047w.hdf'
        self.run_test([filename], 'hdf')

    def test_unrecognised_type(self):
        filenames = [
            '/neodc/nextmap/by_product/ori/sm/sm82/sm82ori.tfw',
            '/neodc/slstr_calibration/sentinel3a/data/instrument_calibration/raw/dpm/2015/05/15/2015_135_20_21__000.ArcRaw'
        ]
        self.run_test(filenames, 'generic')

    def test_badcCSV(self):
        filename = '/badc/faam/data/2013/b807-sep-22/non-core/rhul-cf-gc-irms_faam_20130922_r0_b807_ch4.csv'
        self.run_test([filename], 'badccsv')

    def test_csv(self):
        filename = '/neodc/arsf/2010/EUFAR10_08/EUFAR10_08-2010_301_ValCalHyp/hyperspectral/misc/hawk_bad_pixels_233x320.csv'
        self.run_test([filename], 'generic')

    def test_metadata_tags_file(self):
        filename = '/badc/ukcip02/data/50km_resolution/metadata_tags.json'
        self.run_test([filename], 'metadata')

if __name__ == '__main__':
    unittest.main()
