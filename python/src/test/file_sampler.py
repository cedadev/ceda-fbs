'''
Created on 11 Feb 2016

@author: kleanthis
'''

#!/usr/bin/env python

#29/09/2015 Test code delete after done.       
import os
import fbs_lib.util as util
import sys
 
def sample_files(path):

    #Get basic options.
    current_dir = os.getcwd()

    #Go to directory and create the file list.
    list_of_cache_files = util.build_file_list(path)
    counter = 0

    for filename in list_of_cache_files:
        contents = util.read_file_into_list(filename)
        new_file_name = os.path.join(current_dir, os.path.basename(filename) + "-sample")
        fd = open(new_file_name, "a")
        for item in contents:
            if item.rstrip().endswith(".pp"):
                fd.write(item)
                counter = counter + 1
                if counter > 1000:
                    break

def main():
    print "sampling started."


    sample_files(sys.argv[1])

    print "sampling ended."

if __name__ == '__main__':
    main()