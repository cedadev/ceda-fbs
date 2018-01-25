"""
create_datasets_ini_from_spot.py
---------------------------

Creates the "ceda_all_datasets.ini" file that lists
all "/data" directories under "/badc" and "/neodc"
and other directories such as /edc.

"""

import requests
import os
import sys


def use_data_dir(path):
    """
    Makes sure that if the path associated with the spot is only level 1 directory, checks if there is a data dir and
    appends this to the path for the scan. If path goes past a depth of 1 then just returns the original path.
    
    :param path: Base path to check
    :return: String: Directory path
    """
    if len(path[1:].split(os.sep)) <= 2:
        root, dirs, _ = os.walk(path).next()
        for dir in dirs:
            if dir == 'data':
                return os.path.join(path,'data')

    return path

OUTPUT_FILE = sys.argv[1]

# Download the spot mappings from the cedaarchiveapp
url = "http://cedaarchiveapp.ceda.ac.uk/cedaarchiveapp/fileset/download_conf/"
print "Downloading spotlist from %s" % url
response = requests.get(url)
log_mapping = response.text.split('\n')

# Create output list as a set to make sure each entry is unique
output_list = set()

print "Creating output list"
for line in log_mapping:
    if not line.strip(): continue
    spot, directory = line.strip().split()
    
    # Only add path to output list if it is a real directory
    if os.path.exists(directory):    
        path = use_data_dir(directory)
        output_list.add('{}={}'.format(spot,path))

print "Writing to file"
# Write all spot mappings to file
with open(OUTPUT_FILE,'w') as output:
    outputlist = map(lambda x: x+"\n", sorted(output_list))
    output.writelines(outputlist)




