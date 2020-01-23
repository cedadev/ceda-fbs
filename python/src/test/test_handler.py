import unittest
import os

# Generic
from pwd import getpwuid
from grp import getgrgid
import datetime

# Grib
import xarray as xr

# HDF
from pyhdf.HDF import HDF

# Metadata tags
import json

# NetCDF
import netCDF4
import numpy as np

from fbs.proc.file_handlers.badc_csv_file import BadcCsvFile
from fbs.proc.file_handlers.esasafe_file import EsaSafeFile
from fbs.proc.file_handlers.generic_file import GenericFile
from fbs.proc.file_handlers.grib_file import GribFile
from fbs.proc.file_handlers.hdf_file import HdfFile
from fbs.proc.file_handlers.kmz_file import KmzFile
from fbs.proc.file_handlers.metadata_tags_json_file import MetadataTagsJsonFile
from fbs.proc.file_handlers.nasaames_file import NasaAmesFile
from fbs.proc.file_handlers.netcdf_file import NetCdfFile
from fbs.proc.file_handlers.pp_file import PpFile


class TestBadcCsvFileHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.file_path = 'files/ukmo-metdb_amdars_20161222.csv'

    def setUp(self):
        self.handler = BadcCsvFile(self.file_path, level='1')

    def tearDown(self):
        self.handler = None

    def test_get_file_format(self):
        file_format = self.handler.get_file_format()
        self.assertEqual(file_format, 'BADC CSV')

    def test_csv_parse(self):
        with open(self.file_path, encoding='utf-8', errors='ignore') as fp:
            output = self.handler.csv_parse(fp)

        # Check phenomena found
        self.assertTrue(output[0])

        # Check data found
        self.assertEqual(output[1], '2016-12-22')

        # Check location
        self.assertEqual(output[2], 'global')

    def test_get_phenomena(self):
        with open(self.file_path, encoding='utf-8', errors='ignore') as fp:
            phenomena = self.handler.get_phenomena(fp)

        self.assertTrue(phenomena)

    def test_get_metadata_level2(self):
        response = self.handler.get_metadata_level2()

        self.assertTrue(response[0])
        self.assertIsNotNone(response[1])

    def test_get_metadata_level3(self):
        response = self.handler.get_metadata_level3()

        self.assertTrue(response[0])
        self.assertIsNotNone(response[1])
        self.assertIsNotNone(response[2])

    def test_level_1(self):
        metadata = self.handler.get_metadata()
        self.assertTrue(metadata[0])

    def test_level_2(self):
        self.handler = BadcCsvFile(self.file_path, level='2')
        metadata = self.handler.get_metadata()

        self.assertTrue(metadata[0])
        self.assertIsNotNone(metadata[1])

    def test_level_3(self):
        self.handler = BadcCsvFile(self.file_path, level='3')
        metadata = self.handler.get_metadata()

        self.assertTrue(metadata[0])
        self.assertIsNotNone(metadata[1])
        self.assertIsNotNone(metadata[2])


class TestEsaSafeFileHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.file_path = 'files/S1A_IW_GRDH_1SDV_20171031T061411_20171031T061436_019053_020395_DCCA.manifest'

    def setUp(self):
        self.handler = EsaSafeFile(self.file_path, 1)

    def test_open_file(self):
        self.handler._open_file()
        self.assertTrue(self.handler.sections)

    def test_level_1(self):
        metadata = self.handler.get_metadata()
        self.assertTrue(metadata)

    def test_level_2(self):
        self.handler = EsaSafeFile(self.file_path, 2)
        metadata = self.handler.get_metadata()
        self.assertTrue(metadata)

    def test_level_3(self):
        self.handler = EsaSafeFile(self.file_path, 3)
        metadata = self.handler.get_metadata()

        self.assertIsNone(metadata[1])
        self.assertDictEqual({'coordinates':
                                  {'type': 'envelope',
                                   'coordinates': [[-1.92, 52.839], [2.509, 54.749]]
                                   }
                              },
                             metadata[2])

        self.assertDictEqual({'start_time': '2017-10-31T06:14:11.575373',
                              'end_time': '2017-10-31T06:14:36.575043'},
                             metadata[0]['info']['temporal'])


class TestGenericFileHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.file_path = 'files/nerc-mstrf-radar-mst_capel-dewi_20150901_st300_cardinal_33min-smoothing_v4-0.nc'
        cls.file_meta = {
            'info': {'name': 'nerc-mstrf-radar-mst_capel-dewi_20150901_st300_cardinal_33min-smoothing_v4-0.nc',
                     'name_auto': 'nerc-mstrf-radar-mst_capel-dewi_20150901_st300_cardinal_33min-smoothing_v4-0.nc',
                     'directory': 'files', 'location': 'on_disk', 'user': 'vdn73631', 'group': 'CLRC\\Domain Users',
                     'last_modified': '2019-06-28T12:39:27.839659', 'size': 1389460, 'type': '.nc', 'md5': ''}}

    def test_level_1(self):
        handler = GenericFile(self.file_path, 1)
        metadata = handler.get_metadata()
        print(metadata)
        self.assertDictEqual(self.file_meta, metadata[0])

    def test_level_2(self):
        handler = GenericFile(self.file_path, 2)
        metadata = handler.get_metadata()
        self.assertDictEqual(self.file_meta, metadata[0])

    def test_level_3(self):
        handler = GenericFile(self.file_path, 3)
        metadata = handler.get_metadata()
        self.assertDictEqual(self.file_meta, metadata[0])


class TestGribFileHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.file_path = 'files/FC.w.2008033012.120.grib'
        cls.phenomena = {'long_name': 'Vertical velocity', 'units': 'Pa s**-1',
                         'standard_name': 'lagrangian_tendency_of_air_pressure',
                         'names': ['"Vertical velocity"', '"Vertical velocity"',
                                   '"lagrangian_tendency_of_air_pressure"'], 'best_name': 'Vertical velocity',
                         'agg_string': '"long_name":"Vertical velocity","names":"Vertical velocity";"Vertical velocity";"lagrangian_tendency_of_air_pressure","standard_name":"lagrangian_tendency_of_air_pressure","units":"Pa s**-1"'}
        cls.coordinates = {'coordinates': {'coordinates': [[-180, 90.0], [180, -90.0]]}}
        cls.temporal = {'end_time': '2008-03-30T12:00:00', 'start_time': '2008-03-30T12:00:00'}

        cls.basic = {'info': {'name': 'FC.w.2008033012.120.grib',
                              'name_auto': 'FC.w.2008033012.120.grib',
                              'directory': 'files',
                              'location': 'on_disk',
                              'user': 'vdn73631',
                              'group': 'CLRC\\Domain Users',
                              'last_modified': '2019-05-08T16:30:46.790006',
                              'size': 9456720,
                              'type': '.grib',
                              'md5': '',
                              'format': 'GRIB'}}

    def setUp(self):
        self.handler = GribFile(self.file_path, 1)

    def open_dataset(self):
        return xr.open_dataset(self.file_path, engine='cfgrib', backend_kwargs={'indexpath': ''})

    def test_get_phenomena(self):
        dataset = self.open_dataset()
        metadata = self.handler.get_phenomena(dataset)

        self.assertDictEqual(self.phenomena, metadata[0][0])

    def test_get_geospatial(self):
        dataset = self.open_dataset()
        metadata = self.handler.get_geospatial(dataset)

        self.assertDictEqual(self.coordinates, metadata)

    def test_get_temporal(self):
        dataset = self.open_dataset()
        metadata = self.handler.get_temporal(dataset)

        self.assertDictEqual(self.temporal, metadata)

    def test_level_1(self):
        metadata = self.handler.get_metadata()

        self.assertDictEqual(self.basic, metadata[0])

    def test_level_2(self):
        handler = GribFile(self.file_path, 2)
        metadata = handler.get_metadata()

        file_info = self.basic.copy()
        file_info['info']['read_status'] = 'Successful'

        self.assertDictEqual(metadata[0], file_info)
        self.assertDictEqual(metadata[1][0], self.phenomena)

    def test_level_3(self):
        handler = GribFile(self.file_path, 3)
        metadata = handler.get_metadata()

        file_info = self.basic.copy()
        file_info['info']['read_status'] = 'Successful'
        file_info['info']['temporal'] = self.temporal

        self.assertDictEqual(metadata[0], file_info)
        self.assertDictEqual(metadata[1][0], self.phenomena)
        self.assertDictEqual(metadata[2], self.coordinates)


class TestHdfFileHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.file_path = 'files/c208031b.hdf'
        cls.temporal = {'start_time': '2006-07-27T15:12:37', 'end_time': '2006-07-27T15:13:41'}
        cls.geospatial = {'coordinates': {'type': 'envelope', 'coordinates': [[-3.045, 55.901], [-3.044, 55.941]]}}
        cls.base = {'info': {'name': 'c208031b.hdf', 'name_auto': 'c208031b.hdf',
                             'directory': 'files', 'location': 'on_disk', 'user': 'vdn73631',
                             'group': 'CLRC\\Domain Users', 'last_modified': '2019-06-25T15:35:57.880681',
                             'size': 50445395, 'type': '.hdf', 'md5': '',
                             'format': 'hdf2.'}}
        cls.metadata = ({'info': {'name': 'c208031b.hdf', 'name_auto': 'c208031b.hdf',
                                  'directory': '../../../test/files', 'location': 'on_disk', 'user': 'vdn73631',
                                  'group': 'CLRC\\Domain Users', 'last_modified': '2019-06-25T15:35:57.880681',
                                  'size': 50445395, 'type': '.hdf', 'md5': '', 'read_status': 'Successful',
                                  'temporal': {'start_time': '2006-07-27T15:12:37', 'end_time': '2006-07-27T15:13:41'},
                                  'format': 'hdf2.'}}, None,
                        {'coordinates': {'type': 'envelope', 'coordinates': [[-3.045, 55.901], [-3.044, 55.941]]}})

    def setUp(self):
        self.handler = HdfFile(self.file_path, 1)

    def get_vs(self):
        """

        :return: v, vs
        """
        hdf = HDF(self.file_path)
        return hdf.vgstart(), hdf.vstart()

    def test__get_coords(self):
        v, vs = self.get_vs()
        coords = self.handler._get_coords(vs)
        self.assertTrue(coords)

    def test__get_temporal(self):
        v, vs = self.get_vs()
        temporal = self.handler._get_temporal(vs)
        self.assertDictEqual(temporal, self.temporal)

    def test__parse_timestamps(self):
        time_dict = {'date': '27/07/2006', 'start_time': [151237], 'end_time': [151341]}
        temporal = self.handler._parse_timestamps(time_dict)

        self.assertDictEqual(temporal, self.temporal)

    def test_get_geospatial(self):
        v, vs = self.get_vs()
        geospatial = self.handler.get_geospatial(v, vs)

        self.assertDictEqual(geospatial, self.geospatial)

    def test_get_temporal(self):
        v, vs = self.get_vs()
        temporal = self.handler.get_temporal(v, vs)

        self.assertDictEqual(temporal, self.temporal)

    def test_level_1(self):
        metadata = self.handler.get_metadata()

        self.assertDictEqual(metadata[0], self.base)

    def test_level_2(self):
        handler = HdfFile(self.file_path, 2)
        metadata = handler.get_metadata()
        self.assertDictEqual(metadata[0], self.base)

    def test_level_3(self):
        handler = HdfFile(self.file_path, 3)
        metadata = handler.get_metadata()

        file_info = self.base.copy()
        file_info['info']['read_status'] = 'Successful'
        file_info['info']['temporal'] = self.temporal

        self.assertDictEqual(metadata[0], file_info)
        self.assertIsNone(metadata[1])
        self.assertDictEqual(metadata[2], self.geospatial)


class TestKmzFileHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.file_path = 'files/Alice_GE_2011W06_QM2.kmz'

    def setUp(self):
        self.handler = KmzFile(self.file_path, 1)

    def test_level_1(self):
        pass

    def test_level_2(self):
        pass

    def test_level_3(self):
        pass


class TestMetadataTagsFileHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.file_path = 'files/metadata_tags.json'
        cls.base = {'info': {'name': 'metadata_tags.json', 'name_auto': 'metadata_tags.json', 'directory': 'files',
                             'location': 'on_disk', 'user': 'vdn73631', 'group': 'CLRC\\Domain Users',
                             'last_modified': '2019-06-26T09:58:33.215362', 'size': 2121, 'type': '.json', 'md5': '',
                             'format': 'Metadata tags json'}}
        cls.spatial = {'coordinates': {'type': 'envelope', 'coordinates': [[-11.0, 61], [3, 49]]}}
        cls.temporal = {'start_time': '1961-01-01T00:00:00', 'end_time': '2100-12-31T23:59:59'}
        cls.phenomena = {'units': 'degrees_c', 'long_name': 'Maximum temperature', 'var_id': 'TMAX',
                         'names': ['"Maximum temperature"'], 'best_name': 'Maximum temperature',
                         'agg_string': '"long_name":"Maximum temperature","names":"Maximum temperature","units":"degrees_c","var_id":"TMAX"'}, {
                            'units': 'degrees_c', 'long_name': 'Minimum temperature', 'var_id': 'TMIN',
                            'names': ['"Minimum temperature"'], 'best_name': 'Minimum temperature',
                            'agg_string': '"long_name":"Minimum temperature","names":"Minimum temperature","units":"degrees_c","var_id":"TMIN"'}, {
                            'units': 'degrees_c', 'long_name': 'Daily mean temperature', 'var_id': 'TEMP5',
                            'names': ['"Daily mean temperature"'], 'best_name': 'Daily mean temperature',
                            'agg_string': '"long_name":"Daily mean temperature","names":"Daily mean temperature","units":"degrees_c","var_id":"TEMP5"'}, {
                            'units': 'mm/mth', 'long_name': 'Total precipitation rate', 'var_id': 'PREC',
                            'names': ['"Total precipitation rate"'], 'best_name': 'Total precipitation rate',
                            'agg_string': '"long_name":"Total precipitation rate","names":"Total precipitation rate","units":"mm/mth","var_id":"PREC"'}, {
                            'units': 'mm/day', 'long_name': 'Snowfall rate', 'var_id': 'SNOW',
                            'names': ['"Snowfall rate"'], 'best_name': 'Snowfall rate',
                            'agg_string': '"long_name":"Snowfall rate","names":"Snowfall rate","units":"mm/day","var_id":"SNOW"'}, {
                            'units': 'm/s', 'long_name': 'Wind speed', 'var_id': 'WIND', 'names': ['"Wind speed"'],
                            'best_name': 'Wind speed',
                            'agg_string': '"long_name":"Wind speed","names":"Wind speed","units":"m/s","var_id":"WIND"'}, {
                            'units': '%', 'long_name': 'Relative humidity', 'var_id': 'RHUM',
                            'names': ['"Relative humidity"'], 'best_name': 'Relative humidity',
                            'agg_string': '"long_name":"Relative humidity","names":"Relative humidity","units":"%","var_id":"RHUM"'}, {
                            'units': '%', 'long_name': 'Total cloud in longwave radiation (fraction)',
                            'var_id': 'TCLW', 'names': ['"Total cloud in longwave radiation (fraction)"'],
                            'best_name': 'Total cloud in longwave radiation (fraction)',
                            'agg_string': '"long_name":"Total cloud in longwave radiation (fraction)","names":"Total cloud in longwave radiation (fraction)","units":"%","var_id":"TCLW"'}, {
                            'units': 'W/m', 'long_name': 'Net surface longwave flux', 'var_id': 'NSLW',
                            'names': ['"Net surface longwave flux"'], 'best_name': 'Net surface longwave flux',
                            'agg_string': '"long_name":"Net surface longwave flux","names":"Net surface longwave flux","units":"W/m","var_id":"NSLW"'}, {
                            'units': 'W/m', 'long_name': 'Net surface shortwave flux', 'var_id': 'NSSW',
                            'names': ['"Net surface shortwave flux"'], 'best_name': 'Net surface shortwave flux',
                            'agg_string': '"long_name":"Net surface shortwave flux","names":"Net surface shortwave flux","units":"W/m","var_id":"NSSW"'}, {
                            'units': 'W/m', 'long_name': 'Total downward surface shortwave flux', 'var_id': 'DSWF',
                            'names': ['"Total downward surface shortwave flux"'],
                            'best_name': 'Total downward surface shortwave flux',
                            'agg_string': '"long_name":"Total downward surface shortwave flux","names":"Total downward surface shortwave flux","units":"W/m","var_id":"DSWF"'}, {
                            'units': 'mm', 'long_name': 'Soil moisture content10', 'var_id': 'SMOI',
                            'names': ['"Soil moisture content10"'], 'best_name': 'Soil moisture content10',
                            'agg_string': '"long_name":"Soil moisture content10","names":"Soil moisture content10","units":"mm","var_id":"SMOI"'}, {
                            'units': 'hpa', 'long_name': 'Mean sea level pressure', 'var_id': 'MSLP',
                            'names': ['"Mean sea level pressure"'], 'best_name': 'Mean sea level pressure',
                            'agg_string': '"long_name":"Mean sea level pressure","names":"Mean sea level pressure","units":"hpa","var_id":"MSLP"'}, {
                            'units': 'W/m', 'long_name': 'Surface latent heat flux', 'var_id': 'SLHF',
                            'names': ['"Surface latent heat flux"'], 'best_name': 'Surface latent heat flux',
                            'agg_string': '"long_name":"Surface latent heat flux","names":"Surface latent heat flux","units":"W/m","var_id":"SLHF"'}, {
                            'units': 'g/kg', 'long_name': 'Specific humidity', 'var_id': 'SPHU',
                            'names': ['"Specific humidity"'], 'best_name': 'Specific humidity',
                            'agg_string': '"long_name":"Specific humidity","names":"Specific humidity","units":"g/kg","var_id":"SPHU"'}, {
                            'long_name': 'Inter-annual variability: temperature', 'var_id': 'IAVT',
                            'names': ['"Inter-annual variability: temperature"'],
                            'best_name': 'Inter-annual variability: temperature',
                            'agg_string': '"long_name":"Inter-annual variability: temperature","names":"Inter-annual variability: temperature","var_id":"IAVT"'}, {
                            'long_name': 'Inter-annual variability: precipitation', 'var_id': 'IAVP',
                            'names': ['"Inter-annual variability: precipitation"'],
                            'best_name': 'Inter-annual variability: precipitation',
                            'agg_string': '"long_name":"Inter-annual variability: precipitation","names":"Inter-annual variability: precipitation","var_id":"IAVP"'}

    def open_file(self):
        with open(self.file_path) as reader:
            return json.load(reader)

    def setUp(self):
        self.handler = MetadataTagsJsonFile(self.file_path, 1)

    def test_get_phenomena(self):
        data = self.open_file()
        phenom = self.handler.get_phenomena(data)

        self.assertDictEqual(self.phenomena[0], phenom[0][0])

    def test_get_temporal(self):
        data = self.open_file()
        temporal = self.handler.get_temporal(data)

        self.assertDictEqual(temporal, self.temporal)

    def test_get_geospatial(self):
        data = self.open_file()
        spatial = self.handler.get_geospatial(data)

        self.assertDictEqual(spatial, self.spatial)

    def test_level_1(self):
        metadata = self.handler.get_metadata()

        self.assertDictEqual(metadata[0], self.base)

    def test_level_2(self):
        handler = MetadataTagsJsonFile(self.file_path, 2)
        metadata = handler.get_metadata()

        file_info = self.base.copy()
        file_info['info']['read_status'] = 'Successful'

        self.assertDictEqual(metadata[0], file_info)
        self.assertDictEqual(metadata[1][0][0], self.phenomena[0])

    def test_level_3(self):
        handler = MetadataTagsJsonFile(self.file_path, 3)
        metadata = handler.get_metadata()

        file_info = self.base.copy()
        file_info['info']['read_status'] = 'Successful'
        file_info['info']['temporal'] = self.temporal

        self.assertDictEqual(metadata[0], file_info)
        self.assertDictEqual(metadata[1][0], self.phenomena[0])
        self.assertDictEqual(metadata[2], self.spatial)


class TestNasaAmesFileHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.file_path = 'files/arsf-aimms20_arsf-dornier_20081102_r0_307_voc-05.na'
        cls.base = {'info': {'name': 'arsf-aimms20_arsf-dornier_20081102_r0_307_voc-05.na',
                             'name_auto': 'arsf-aimms20_arsf-dornier_20081102_r0_307_voc-05.na', 'directory': 'files',
                             'location': 'on_disk', 'user': 'vdn73631', 'group': 'CLRC\\Domain Users',
                             'last_modified': '2019-06-27T16:26:54.399195', 'size': 2754478, 'type': '.na', 'md5': '',
                             'format': 'NASA Ames'}}

        cls.phenomena = [{'units': 'degrees C', 'names': ['"Temperature"'], 'best_name': 'Temperature',
                          'agg_string': '"names":"Temperature","units":"degrees C"'},
                         {'units': 'percent', 'names': ['"Relative Humidity"'], 'best_name': 'Relative Humidity',
                          'agg_string': '"names":"Relative Humidity","units":"percent"'},
                         {'units': 'Pascals', 'names': ['"Pressure"'], 'best_name': 'Pressure',
                          'agg_string': '"names":"Pressure","units":"Pascals"'},
                         {'units': 'ms-1', 'names': ['"U"'], 'best_name': 'U',
                          'agg_string': '"names":"U","units":"ms-1"'},
                         {'units': 'ms-1', 'names': ['"V"'], 'best_name': 'V',
                          'agg_string': '"names":"V","units":"ms-1"'},
                         {'units': 'degrees', 'names': ['"Latitude"'], 'best_name': 'Latitude',
                          'agg_string': '"names":"Latitude","units":"degrees"'},
                         {'units': 'degrees', 'names': ['"Longitude"'], 'best_name': 'Longitude',
                          'agg_string': '"names":"Longitude","units":"degrees"'},
                         {'units': 'metres', 'names': ['"Altitude"'], 'best_name': 'Altitude',
                          'agg_string': '"names":"Altitude","units":"metres"'},
                         {'units': 'ms-1', 'names': ['"Eastward wind"'], 'best_name': 'Eastward wind',
                          'agg_string': '"names":"Eastward wind","units":"ms-1"'},
                         {'units': 'ms-1', 'names': ['"Northward wind"'], 'best_name': 'Northward wind',
                          'agg_string': '"names":"Northward wind","units":"ms-1"'},
                         {'units': 'ms-1', 'names': ['"Turbulence"'], 'best_name': 'Turbulence',
                          'agg_string': '"names":"Turbulence","units":"ms-1"'},
                         {'units': 'degrees', 'names': ['"Aircraft roll angle"'], 'best_name': 'Aircraft roll angle',
                          'agg_string': '"names":"Aircraft roll angle","units":"degrees"'},
                         {'units': 'degrees', 'names': ['"Aircraft pitch angle"'], 'best_name': 'Aircraft pitch angle',
                          'agg_string': '"names":"Aircraft pitch angle","units":"degrees"'},
                         {'units': 'degrees', 'names': ['"Aircraft heading"'], 'best_name': 'Aircraft heading',
                          'agg_string': '"names":"Aircraft heading","units":"degrees"'},
                         {'units': 'ms-1', 'names': ['"TAS"'], 'best_name': 'TAS',
                          'agg_string': '"names":"TAS","units":"ms-1"'},
                         {'units': 'degrees', 'names': ['"Aircraft yaw angle (slip)"'],
                          'best_name': 'Aircraft yaw angle (slip)',
                          'agg_string': '"names":"Aircraft yaw angle (slip)","units":"degrees"'}]

    def setUp(self):
        self.handler = NasaAmesFile(self.file_path, 1)

    def test_phenomena(self):
        phenom = self.handler.phenomena()
        self.assertEqual(len(phenom), 16)

    def test_get_phenomena(self):
        phenom = self.handler.get_phenomena()
        self.assertEqual(self.phenomena, phenom[0])

    def test_level_1(self):
        metadata = self.handler.get_metadata()
        self.assertDictEqual(metadata[0], self.base)

    def test_level_2(self):
        handler = NasaAmesFile(self.file_path, 2)
        metadata = handler.get_metadata()

        file_info = self.base.copy()
        file_info['info']['read_status'] = 'Successful'

        self.assertDictEqual(metadata[0], file_info)
        self.assertEqual(metadata[1], self.phenomena)

    def test_level_3(self):
        handler = NasaAmesFile(self.file_path, 3)
        metadata = handler.get_metadata()

        file_info = self.base.copy()
        file_info['info']['read_status'] = 'Successful'

        self.assertDictEqual(metadata[0], file_info)
        self.assertEqual(metadata[1], self.phenomena)
        self.assertIsNone(metadata[2])


class TestNetcdfFileHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.file_name = 'files/nerc-mstrf-radar-mst_capel-dewi_20150901_st300_cardinal_33min-smoothing_v4-0.nc'
        cls.temporal = {'start_time': '2015-09-01T00:04:11', 'end_time': '2015-09-01T23:57:06'}
        cls.coordinates = {'coordinates': {'type': 'Point', 'coordinates': [-4.005469799041748, 52.42452621459961]}}
        cls.base = {'info': {'name': 'nerc-mstrf-radar-mst_capel-dewi_20150901_st300_cardinal_33min-smoothing_v4-0.nc',
                             'name_auto': 'nerc-mstrf-radar-mst_capel-dewi_20150901_st300_cardinal_33min-smoothing_v4-0.nc',
                             'directory': 'files', 'location': 'on_disk', 'user': 'vdn73631',
                             'group': 'CLRC\\Domain Users', 'last_modified': '2019-06-28T12:39:27.839659',
                             'size': 1389460, 'type': '.nc', 'md5': '', 'format': 'NetCDF'}}

    def setUp(self):
        self.handler = NetCdfFile(self.file_name, 1)
        self.file = netCDF4.Dataset(self.file_name)

    def tearDown(self):
        self.file.close()

    def test_clean_coordinate(self):
        input_coords = [2.1, 2.2, 0.0, 'nan']
        output = list(filter(self.handler.clean_coordinate, input_coords))

        self.assertEqual(output, [2.1, 2.2])

    def test_geospatial(self):
        geospatial = self.handler.geospatial(self.file, 'latitude', 'longitude')
        self.assertEqual(geospatial['type'], 'track')
        self.assertEqual(list(geospatial.keys()), ['type', 'lat', 'lon'])

    def test_find_var_by_standard_name(self):
        self.assertEqual(self.handler.find_var_by_standard_name(self.file, 'latitude'), 'latitude')
        self.assertEqual(self.handler.find_var_by_standard_name(self.file, 'longitude'), 'longitude')
        self.assertEqual(self.handler.find_var_by_standard_name(self.file, 'time'), 'time')

    def test_get_geospatial(self):
        geospatial = self.handler.get_geospatial(self.file)
        self.assertEqual(geospatial['type'], 'track')
        self.assertEqual(list(geospatial.keys()), ['type', 'lat', 'lon'])

    def test_get_temporal(self):
        time = self.handler.get_temporal(self.file)
        self.assertDictEqual(time, self.temporal)

    def test_get_phenomena(self):
        phenom = self.handler.get_phenomena(self.file)
        self.assertEqual(len(phenom[0]), 21)

    def test_level_1(self):
        info = self.handler.get_metadata()
        self.assertDictEqual(info[0], self.base)

    def test_level_2(self):
        handler = NetCdfFile(self.file_name, 2)
        meta = handler.get_metadata()

        file_info = self.base.copy()
        file_info['info']['read_status'] = 'Successful'

        self.assertDictEqual(meta[0], file_info)
        self.assertEqual(len(meta[1]), 21)

    def test_level_3(self):
        handler = NetCdfFile(self.file_name, 3)
        meta = handler.get_metadata()

        file_info = self.base.copy()
        file_info['info']['read_status'] = 'Successful'
        file_info['info']['temporal'] = self.temporal

        self.assertDictEqual(meta[0], file_info)
        self.assertEqual(len(meta[1]), 21)
        self.assertDictEqual(meta[2], self.coordinates)


class TestPpFileHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.file_path = os.path.abspath('files/affp2006070218_05201_33.pp')
        cls.dir = os.path.dirname(cls.file_path)

        file_stats = os.stat(cls.file_path)

        cls.uid = getpwuid(file_stats.st_uid).pw_name
        cls.gid = getgrgid(file_stats.st_gid).gr_name

        cls.last_modified = datetime.datetime.fromtimestamp(file_stats.st_mtime).isoformat()

        cls.size = os.path.getsize(cls.file_path)

    def test_level_1(self):
        expected = (
            {'info': {'name': 'affp2006070218_05201_33.pp',
                      'name_auto': 'affp2006070218_05201_33.pp',
                      'directory': self.dir,
                      'location': 'on_disk',
                      'user': self.uid,
                      'group': self.gid,
                      'last_modified': self.last_modified,
                      'size': self.size,
                      'type': '.pp',
                      'md5': '',
                      'format': 'PP',
                      }},
        )

        handler = PpFile(self.file_path, '1')
        metadata = handler.get_metadata()

        self.assertEqual(expected, metadata)

    def test_level_2(self):
        expected = (
            {'info': {'name': 'affp2006070218_05201_33.pp',
                      'name_auto': 'affp2006070218_05201_33.pp',
                      'directory': self.dir,
                      'location': 'on_disk',
                      'user': self.uid,
                      'group': self.gid,
                      'last_modified': self.last_modified,
                      'size': self.size,
                      'type': '.pp',
                      'md5': '',
                      'read_status': 'Successful',
                      'format': 'PP',
                      }},
            [
                {
                    'standard_name': 'convective_rainfall_amount',
                    'units': 'kg m-2',
                    'names': ['"convective_rainfall_amount"'],
                    'best_name': 'convective_rainfall_amount',
                    'agg_string': '"names":"convective_rainfall_amount","standard_name":"convective_rainfall_amount","units":"kg m-2"'
                }
            ]
        )

        handler = PpFile(self.file_path, '2')
        metadata = handler.get_metadata()

        self.assertDictEqual(expected[0], metadata[0])
        self.assertEqual(len(expected[1]), 1)

    def test_level_3(self):
        expected = (
            {'info': {'name': 'affp2006070218_05201_33.pp',
                      'name_auto': 'affp2006070218_05201_33.pp',
                      'directory': self.dir,
                      'location': 'on_disk',
                      'user': self.uid,
                      'group': self.gid,
                      'last_modified': self.last_modified,
                      'size': self.size,
                      'type': '.pp',
                      'md5': '',
                      'temporal': {
                          'start_time': '2006-07-03T10:30:00',
                          'end_time': '2006-07-03T10:30:00'
                      },
                      'read_status': 'Successful',
                      'format': 'PP',
                      }},
            [
                {
                    'standard_name': 'convective_rainfall_amount',
                    'units': 'kg m-2',
                    'names': ['"convective_rainfall_amount"'],
                    'best_name': 'convective_rainfall_amount',
                    'agg_string': '"names":"convective_rainfall_amount","standard_name":"convective_rainfall_amount","units":"kg m-2"'
                }
            ],
            {'coordinates': {
                'type': 'envelope',
                'coordinates': [[-20.0, -37.5], [57.58, 40.08]]}
            }
        )

        handler = PpFile(self.file_path, '3')
        metadata = handler.get_metadata()

        self.assertDictEqual(expected[0], metadata[0])
        self.assertEqual(len(expected[1]), 1)
        self.assertDictEqual(expected[2], metadata[2])


if __name__ == '__main__':
    unittest.main()
