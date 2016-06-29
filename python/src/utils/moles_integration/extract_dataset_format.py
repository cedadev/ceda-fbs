#!/usr/bin/env python
"""
Usage:
  extract_dataset_format.py -h | --help
  extract_dataset_format.py --version
  extract_dataset_format.py [-f <filename> | --filename <filename>]

Options:
  -h --help                                  Show this screen.
  --version                                  Show version.
  -f --filename=<filename>                   File from where the dataset [default: datasets.ini].
 """

import src.fbs_api as fbs_api
import src.fbs_lib.util as util
from docopt import docopt

def create_html_table(data):

    rows = ""
    for item in data:
        row = "<tr><td bgcolor=\"#F5F5DC\">{}</td><td bgcolor=\"#F5F5DC\">{}</td><td bgcolor=\"#F5F5DC\">{}</td></tr>\n".format(item[0], ', '.join(item[1]), ', '.join(item[2]))
        rows += row


    table = "<table border=\"1\" >\
             <tr><th bgcolor=\"#D2691E\"><h2>Path</h2></th>\
             <th bgcolor=\"#D2691E\"><h2>Formats found</h2></th>\
             <th bgcolor=\"#D2691E\"><h2>Example files</h2></th>\
             </tr>\
             %s\
             </table>\
            " %rows

    html_page ="<!DOCTYPE html>\
               <html>\
               <head>\
               </head>\
               <body>\
               %s\
               </body>\
               </html>" %table

    util.save_to_file("html_file.htm", html_page)
    return table

def process_obs_to_html(dataset_file):
    """
    Looks up each Observation in the MOLES catalogue, matches phenomena to it 
    from ES and then writes HTML pages listing them. 
    """
    lines = util.read_file_into_list(dataset_file)
    summary_info = []
    counter = 0
    for line in lines:
        path = line.split("=")[1].rstrip()
        try:
            print "searching path {}".format(path)
            results = fbs_api.get_dir_info(path)
        except:
            continue

        #if len(results["formats"]) > 0:
            #print "Formats in directory {} are {} and some files {}".format(data_path, results["formats"], results["sample_names"])
        record = (line, results["formats"], results["sample_names"])
        summary_info.append(record)
        counter += 1
        if counter >10:
            break

    print create_html_table(summary_info)

if __name__ == "__main__":
    com_args = util.sanitise_args(docopt(__doc__))
    process_obs_to_html(com_args["filename"])
