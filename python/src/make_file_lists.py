#!/usr/bin/env python

"""
Usage:
  make_file_lists.py -h | --help
  make_file_lists.py --version
  make_file_lists.py (-f <filename> | --filename <filename>) (-m <location> | --make-list <location>)
                     [-p <number_of_processes> | --num-processes <number_of_processes>]  
                     
Options:
  -h --help                                  Show this screen.
  --version                                  Show version.
  -f --filename=<filename>                   File from where the dataset will be read [default: datasets.ini]. 
  -m --make-list=<location>                  Stores the list of filenames to a file.
  -p --num-processes=<number_of_processes>   Number of processes to use.
 """

import os

from docopt import docopt

import ceda_di.util.util as util
from ceda_di import __version__  # Grab version from package __init__.py
from ceda_di.extract import Extract_seq
from ceda_di.index import BulkIndexer
from ceda_di.search import Searcher
from operator import or_

import glob
import logging
import logging.handlers
import datetime   
from enum import Enum
import sys


Script_status = Enum( "Script_status",
                      "create_lists"
                    )    
    

def set_program_op_status_and_defaults(com_args):
    
    """
    Set global variables that determine the operations to be performed. 
    """
     
    status_and_defaults = []   
    # Searches for the configuration file.
    if 'config' not in com_args or not com_args["config"]:
        direc = os.path.dirname(__file__)
        conf_path = os.path.join(direc, "../config/ceda_di.json")
        com_args["config"] = conf_path

    #Creates a dictionary with default settings some of them where loaded from th edefaults file.
    config = util.get_settings(com_args["config"], com_args)
    status_and_defaults.append(config)       
    status_and_defaults.append(Script_status.create_lists)
    
    
    return status_and_defaults


def create_file_lists(status, config):
    
    """
    Find and store all files belonging to each dataset. 
    """
    
    #Get file.
    filename = config["filename"]
    #Extract datasets ids and paths.
    datasets =  util.find_dataset(filename, "all")
    datasets_ids = datasets.keys()
    num_datasets = len(datasets_ids)
    scan_commands = []
    current_dir = os.getcwd() 
    directroy_to_save_files = config["make-list"]
    
    #Create the commands that will create the files containing the paths to data files. 
    for i in range(0, num_datasets):
            
        command = "python " + current_dir + "/scan_dataset.py -f "\
                  + filename + " -d " + datasets_ids[i] + " --make-list " + directroy_to_save_files + "/" + datasets_ids[i] + "_dataset__files.txt"   
        
        scan_commands.append(command)          
  
    
    lotus_max_processes = config["num-processes"] 
    
    #Run each command in lotus.
    util.run_tasks_in_lotus(scan_commands, int(lotus_max_processes), user_wait_time=3)
  
def main():
        
    """
    Relevant to ticket :
    http://team.ceda.ac.uk/trac/ceda/ticket/23217
    """   
      
    #Get command line arguments. 
    com_args = util.sanitise_args(docopt(__doc__, version=__version__))        
       
    #Insert defaults
    status_and_defaults = set_program_op_status_and_defaults(com_args)      
     
   
   
    start = datetime.datetime.now()              
    print "Script started at:" +str(start) 
       
    status = status_and_defaults[1]
    config = status_and_defaults[0]   
     
    #Create files containing files  
    create_file_lists(status, config)
     
    end = datetime.datetime.now()    
    print "Script ended at :" + str(end) + " it ran for :" + str(end - start) 
        
        
if __name__ == '__main__':
    main()