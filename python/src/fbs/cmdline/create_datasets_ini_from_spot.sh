#!/bin/bash

# create_datasets_ini_file.sh
# ---------------------------
#
# Creates the "ceda_all_datasets.ini" file that lists
# directories contained in spots as listed at: 
# http://cedaarchiveapp.ceda.ac.uk/cedaarchiveapp/fileset/download_conf/
# 
# 

OUT_FILE=ceda_all_datasets.ini

python $BASEDIR/ceda-fbs/python/src/fbs/cmdline/create_datasets_ini_from_spot.py $OUT_FILE

echo "Wrote datasets file to: $OUT_FILE"
