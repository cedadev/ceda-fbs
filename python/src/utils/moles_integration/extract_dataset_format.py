#!/usr/bin/env python

"""
extract_dataset_format.py
==========================
"""

import urllib, sys
sys.path.append(".")
import src.fbs_api as fbs_api
import src.fbs_lib.util as util

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

def process_obs_to_html(paths_page="http://catalogue.ceda.ac.uk/export/paths/"):
    """
    Looks up each Observation in the MOLES catalogue, matches phenomena to it 
    from ES and then writes HTML pages listing them. 
    """
    lines = urllib.urlopen(paths_page).readlines()
    lines.sort()
    n = len(lines)
    summary_info = []
    test_counter = 0

    SPLIT = 100

    while lines:
        lines_to_process = lines[:SPLIT]
        lines = lines[SPLIT:]

        test_counter = test_counter +1
        if test_counter > 4:
            break

        for i, line in enumerate(lines_to_process):
            if i > 50000000: 
                lines = []
                break

            data_path, ob_url = line.strip().split()


            try:
                results = fbs_api.get_dir_info(data_path)
            except:
                continue

            if len(results["formats"]) > 0:
                #print "Formats in directory {} are {} and some files {}".format(data_path, results["formats"], results["sample_names"])
                record = (data_path, results["formats"], results["sample_names"])
                summary_info.append(record)


    print create_html_table(summary_info)

if __name__ == "__main__":

    process_obs_to_html() 
