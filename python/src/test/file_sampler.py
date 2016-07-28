'''
Created on 11 Feb 2016

@author: kleanthis
'''

#!/usr/bin/env python

#29/09/2015 Test code delete after done.       
import os
import src.fbs.processing.common_util.util as util
import sys
 
def sample_files(in_path, out_path):

    #Get basic options.

    #Go to directory and create the file list.
    list_of_cache_files = util.build_file_list(in_path)
    counter = 0

    for filename in list_of_cache_files:
        contents = util.read_file_into_list(filename)
        new_file_name = os.path.join(out_path, os.path.basename(filename) + "-sample")
        fd = open(new_file_name, "a")
        for item in contents:
            if item.rstrip().endswith(".pp"):
                fd.write(item)
                counter = counter + 1
                if counter > 1000:
                    break

def main():
    print "sampling started."


    sample_files(sys.argv[1], sys.argv[2])

    print "sampling ended."

if __name__ == '__main__':
    main()