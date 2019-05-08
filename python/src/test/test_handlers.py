# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '07 May 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from fbs.proc.file_handlers.handler_picker import HandlerPicker
import os
from fbs.proc.common_util.util import cfg_read

test_files = [
    '/badc/ukmo-metdb/data/amdars/2016/12/ukmo-metdb_amdars_20161222.csv',
    '/neodc/sentinel1a/data/IW/L1_GRD/h/IPF_v2/2017/10/31/S1A_IW_GRDH_1SDV_20171031T061411_20171031T061436_019053_020395_DCCA.manifest',
    '/badc/ecmwf-for/slimcat/data/2012/11/spam2012110318u.grb',
    '/neodc/arsf/2006/GB05_01/GB05_01-2006_208_Inveresk/L1b/c208031b.hdf',
    '/neodc/sister/data/QM2/KML/2011/Alice_GE_2011W06_QM2.kmz',
    '/badc/ukcip02/data/50km_resolution/metadata_tags.json',
    '/neodc/arsf/2008/VOC_05/VOC_05-2008_307_Chile/aimms/arsf-aimms20_arsf-dornier_20081102_r0_307_voc-05.na',
    '/badc/mst/data/nerc-mstrf-radar-mst/v4-0/st-mode/cardinal/2015/09/nerc-mstrf-radar-mst_capel-dewi_20150901_st300_cardinal_33min-smoothing_v4-0.nc',
    '/badc/amma/data/ukmo-nrt/africa-lam/pressure_level_split/af/fp/2006/07/02/affp2006070218_05201_33.pp',
]

conf = cfg_read('../../config/ceda-fbs.ini')

files = os.listdir('files')
handler_picker = HandlerPicker(conf)

for file in files:

    file_path = os.path.join('files', file)
    handler = handler_picker.pick_best_handler(os.path.join(file_path))


    print()
    print(handler)
    for level in range(1,4):
        print(handler(file_path, str(level)).get_metadata())


