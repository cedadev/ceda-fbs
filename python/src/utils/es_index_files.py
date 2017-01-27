#!/usr/bin/env python

"""
Usage:
  es_index_files.py -h | --help
  es_index_files.py (-d <directory>)
  es_index_files.py (-l <list_file>)

Options:
  -h --help             Show this screen.
  -d <directory>        Directory to scan. 
  -l <list_file>        File containing a list of file paths (one per line).
 """

import sys
import getopt
import datetime
import subprocess
import os
import shlex
import urllib2
import time
import ConfigParser

# Default locations for script and virtual env.
# Get base directory from script location
BASE_DIR = "/".join(os.path.realpath(__file__).split("/")[:-5])
src_dir = os.path.join(BASE_DIR, "ceda-di/python/src")
virtual_env = os.path.join(BASE_DIR, "venv-ceda-di/bin/python2.7")
CONFIG_FILE = os.path.join(BASE_DIR, "ceda-fbs/python/config/ceda_fbs.ini")


def sanitise_args(config):
    """
    Sanitise command-line configuration.
    :param config: Config dictionary (from docopt)
    :returns: Config dictionary with all keys stripped of '<' '>' and '--'
    """
    sane_conf = {}
    for key, value in config.iteritems():
        if value is not None:
            key = key.lstrip("-><").rstrip("><")
            sane_conf[key] = value

    return sane_conf


def execute_command(cmd, url):
    print "Running command: %s" % cmd    
    subprocess.check_output(shlex.split(cmd), cwd=src_dir, env=os.environ.copy())

    try:
        subprocess.check_output(shlex.split(cmd), cwd=src_dir, env=os.environ.copy())
    except subprocess.CalledProcessError as grepexc:                                                                                                   
        print "Error code:", grepexc.returncode, grepexc.output
    else:
        time.sleep(2)
        report_files(url) 


def construct_url(conf_file):
    conf = ConfigParser.ConfigParser()
    conf.read(conf_file)

    es_host = conf.get("es-configuration", "es-host")
    es_port = conf.get("es-configuration", "es-port")
    es_index = conf.get("es-configuration", "es-index")
    url= "http://{}:{}/{}/_count?q=file.filename:*&pretty=true".format(es_host, es_port, es_index)

    print "URL used: %s" % url
    return url


def report_files(url):
    content = urllib2.urlopen(url).read()
    print "\nFile count in index: %s" % content.split(",")[0].replace("{","")


def es_scan(directory):
    cmd = "{}/scan_dataset.py ????? --config {} --send-to-index {}".format(src_dir, CONFIG_FILE, directory)
    url = construct_url(CONFIG_FILE)
    execute_command(cmd, url)


def scan_file(list_file):
    with open(list_file) as fd:
        lines = fd.readlines()

    for line in lines:
        es_scan(line)


def main():
    """
    Main controller script.
    """
    # Get command line arguments.
    config = {"directory": None, "file_list": None}
    args, dummy = getopt.getopt(sys.argv[1:], "d:l:h")

    for k, v in args:
        if k == "-d":
            config["directory"] = v
        elif k == "-l":
            config["file_list"] = v
        elif k == "-h":
            print __doc__
            sys.exit() 
            
    start = datetime.datetime.now()
    print "Script started at: %s" % start

    if config["file_list"]:
        scan_file(config["file_list"])
    elif config["directory"]:
        es_scan(config["directory"])
    else:
        print __doc__
        print "Arguments not recognised!"
    
    end = datetime.datetime.now()
    print "Script ended at: %s ; it ran for: %s seconds." % (str(end), str(end - start))


if __name__ == '__main__':

    main()
