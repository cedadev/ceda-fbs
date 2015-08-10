#!/usr/bin/env python

"""
Usage:
  scan_dataset.py -h | --help
  scan_dataset.py --version
  scan_dataset.py (-f <filename> | --filename <filename>) (-d <dataset_id> | --dataset <dataset_id> ) 
                  (-l <level> | --level <level>) 
                  [-m <location> | --make-list <location>]  
                  [-c <path_to_config_dir> | --config <path_to_config_dir>] 
                  [-n <n_files> | --num-files <n_files>] 
                  [-s <start_number> | --start <start_number>]
  
Options:
  -h --help                           Show this screen.
  --version                           Show version.
  -d --dataset=<dataset_id>           Dataset id.
  -f --filename=<filename>            File from where the dataset will be read [default: datasets.ini]. 
  -l --level=<level>                  Level of search:
                                      Level 1: File names and sizes
                                      Level 2: File names, sizes and phenomena (e.g. "air temperature")
                                      Level 3: File names, sizes, phenomena and geospatial metadata.
  -m --make-list=<make-list>          Stores the list of filenames to a file.
  -c --config=<path_to_config_dir>    Specify the main configuration directory.
  -n --num-files=<n_files>            Number of files to scan.
  -s --start=<start_number>           Starting point within the cache file containing filenames [default: 10].
  
 """

import os

from docopt import docopt

import ceda_di.util.util as util
from ceda_di import __version__  # Grab version from package __init__.py
from ceda_di.extract import Extract
from ceda_di.index import BulkIndexer
from ceda_di.search import Searcher
from operator import or_

import glob
import logging
import logging.handlers   


search_dir_and_store_to_file = None 
search_dir_and_store_to_database = None 

def sd_args_validity_ckeck(args_dict):
    """
    checks the validity of command line arguments
    :param dictionary containing ags.
    :returns: 
    """
    level = int(args_dict.get("level"))
    if level < 1  or level > 3:
        raise NameError('value out of range')
 
    num_files = args_dict.get("num-files")
    if num_files:
        num_files = int(num_files)
         
    start_number = args_dict.get("start")
    if start_number:
        start_number = int(args_dict.get("start"))
       
    #TODO : Add more cases here...    

def sd_find_dataset(file, dataset_id):
    """
     Returns the path of the given dataset id. 
    """
    vars = {}
    with open(file) as l_file:
        for line in l_file:
            name, var = line.partition("=")[::2]
            vars[name.strip()] = var.strip()
    
    return vars[dataset_id]        

def sd_scan_and_store_to_db(conf):
    
    """
    Reads files from the filesystem and outputs metadata to database.
    """ 
    
    # Finds the directory to be scanned 
    dataset_ids_file_path = conf.get("filename")
    dataset_id = conf.get("dataset")
    path = sd_find_dataset(dataset_ids_file_path, dataset_id) #derectory where the files to be searched are.
    num_files = conf.get("num-files")
    
    # Extracts metadata and stores then in elastic search.
    search_level = conf.get("level")
    if dataset_ids_file_path and dataset_id and search_level :
        extract = Extract(conf, path, "seq")
        extract.run_seq(search_level, num_files);
              
def sd_scan_and_store_to_file(conf_args):
    """
    Reads files from filesystem and stores their filenames to a file.
    """ 
        
    dataset_ids_file_path = conf_args.get("filename")
    dataset_id = conf_args.get("dataset")
    path_to_files = sd_find_dataset(dataset_ids_file_path, dataset_id) #derectory where the files to be searched are.
    file_to_store_filenames = conf_args.get("make-list")
    list = util.build_file_list(path_to_files)
    util.write_list_to_file(list, file_to_store_filenames)         
                
        
def sd_set_program_op_status_and_defaults(conf_args):
    
    global search_dir_and_store_to_file
    global search_dir_and_store_to_database
    
    
    # Searches for the configuration file.
    if 'config' not in conf_args or not conf_args["config"]:
        direc = os.path.dirname(__file__)
        conf_path = os.path.join(direc, "../config/ceda_di.json")
        conf_args["config"] = conf_path

    #Creates a dictionary with default settings some of them where loaded from th edefaults file.
    config = util.get_settings(conf_args["config"], conf_args)

    
       
    if ("make-list" in conf_args) and ("dataset" in conf_args) and  ("filename" in conf_args) :
        search_dir_and_store_to_file = True
    elif  ("dataset" in conf_args) and  ("filename" in conf_args) and ("level" in conf_args) :
        search_dir_and_store_to_database = True 
    
    return config 
    
def main():
        
    """
    Basic algorithm :
        Validate input
        Locate directory to be scanned
        Create file list
        Extract data
        Post data to elastic search. 
        Also handle other options.
    """   
     
    conf_args = util.sanitise_args(docopt(__doc__, version=__version__))        
         
    try: 
        sd_args_validity_ckeck(conf_args)
    except ValueError as verr:
        log.error("Error in configuration.")
        return  
    except Exception as ex:
        log.error("Error in configuration")
        return 
    
    config_file = sd_set_program_op_status_and_defaults(conf_args)
    
    if search_dir_and_store_to_file :
        sd_scan_and_store_to_file(config_file)
    elif  search_dir_and_store_to_database :
         sd_scan_and_store_to_db(config_file)     
               
        
if __name__ == '__main__':
    main()