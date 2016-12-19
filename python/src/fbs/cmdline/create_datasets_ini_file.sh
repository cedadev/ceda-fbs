#!/bin/bash

# create_datasets_ini_file.sh
# ---------------------------
#
# Creates the "ceda_all_datasets.ini" file that lists
# all "/data" directories under "/badc" and "/neodc".
#

OUT_FILE=ceda_all_datasets.ini
TMP_FILE=${OUT_FILE}.tmp

items=$(find -L /badc /neodc -maxdepth 2 -name data | sort -u)

EXCLUSIONS="(ARCHIVE_INFO|ftpaccess|requests|testing)"

for i in $items; do
    if [ ! $(echo $i | grep -P "$EXCLUSIONS") ]; then
        id=$(echo $i | cut -c2- | cut -d/ -f1,2 | sed 's/\//__/g')
        echo ${id}=${i}
    fi
done > $TMP_FILE

cat $TMP_FILE | sort -u > $OUT_FILE
rm $TMP_FILE

echo "Wrote datasets file to: $OUT_FILE"
