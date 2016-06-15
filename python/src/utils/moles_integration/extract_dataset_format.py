#!/usr/bin/env python

"""
extract_dataset_format.py
==========================
"""

import urllib, re, os, sys
sys.path.append(".")
import src.fbs_api as fbs_api
import sys
import time
from elasticsearch import Elasticsearch
import datetime
import src.fbs_lib.util as util
import os

TIME_UNIT_REGEX = re.compile("^(years|months|weeks|days|hours|minutes|seconds)\s+since\s+\d+")
VERBOSE = False
OUT_DIR = "htmls"
if not os.path.isdir(OUT_DIR): os.mkdir(OUT_DIR)

def read_cfg():

    c_dir     = os.path.dirname(__file__)
    conf_path = os.path.join(c_dir, "../../../config/ceda_fbs.ini")
    config    = util.cfg_read(conf_path)

    return config

def open_connection(cfg):

    host = cfg["es-configuration"]["es-host"].split(",")[0]
    port = cfg["es-configuration"]["es-port"]
    es_conn = Elasticsearch(hosts=[{"host": host, "port": port}])

    return es_conn

def find_doc(cfg, directory):

    query =\
    {
     "query" : 
     {
      "wildcard" : 
      {
       "info.directory" : "{}*".format(directory)
      }
     }
    }

    #open connection.
    es_conn = open_connection(cfg)

    es_index = cfg["es-configuration"]["es-index"]
    es_type = cfg["es-configuration"]["es-mapping"].split(",")[0]


    res = es_conn.search( index=es_index,
                          doc_type=es_type,
                          body=query,
                          request_timeout=60
                        )

    #{u'count': 27, u'_shards': {u'successful': 5, u'failed': 0, u'total': 5}}
    return res[u"hits"]


PAGE_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css">
  <script src="https://ajax.googleapis.com/ajax/libs/jquery/1.12.2/jquery.min.js"></script>
  <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/js/bootstrap.min.js"></script>
  <title>%(title)s</title>
</head>

    <body>
<div class="container">
        <h1>%(title)s 
            <a href="%(previous_link)s"><span class="glyphicon glyphicon-triangle-left"></span></a>
            <a href="%(next_link)s"><span class="glyphicon glyphicon-triangle-right"></span></a>
        </h1>

            <table class="table table-striped">
                <thead>
                <tr>
                    <th>MOLES UUID</th>
                    <th>Path</th>
                    <th>Formats</th>
                    <th>M of N Phens</th>
                    <th>Phenomenon</th>
                </tr>
                </thead>
                <tbody>
%(content)s
                </tbody>
            </table>
</div>
    </body>

</html>"""


def create_phens_list(phens):
    """
    Generate a list of phenomena from the ES response.
    """
    phens_list = []

    for phen in phens:
        atts = phen["_source"]["attributes"]
        d = {}

        for att in atts:
            key = att["name"]
            value = att["value"]
            d[key] = value

        phens_list.append(d)

    return phens_list

def check_filter_out(phen):
    "Return True if phenonemon should not be shown or False."
    if "units" in phen.keys():
        units = phen["units"]
        if TIME_UNIT_REGEX.match(units):
            return True
        if units in ("degrees_north", "degrees_east"):
            return True 

def render_results(ob_url, data_path, results, count, total):
    """
    Returns rendered HTML for the phenomena related to a MOLES Observation.
    """
    generic_html = '<td><a href="%s">%s</td><td>%s</td>' % (ob_url, ob_url.rstrip("/").split("/")[-1], data_path)
    if VERBOSE: print "\n\nOb: %s (%d of %d); Path: %s" % (ob_url, count, total, data_path)
    if VERBOSE: print "Size: %s bytes; N files: %s, Formats: %s" % (results['total_size'], results['number_of_files'],
                                                        results['formats'])
    if VERBOSE: print "Phenomena: (%d)" % len(results["phenomena"])
    formats = ", ".join(results["formats"])
    generic_html += "<td>%s</td>" % formats

    try:
      phens = create_phens_list(results["phenomena"])
    except Exception, err:
      if VERBOSE: print err
      if VERBOSE: print "\tNO RESULTS FOR: %s" % data_path
      phens = []

    n_phens = len(phens)
    rows = []

    good_phens = [phen for phen in phens if not check_filter_out(phen)]

    for i, phen in enumerate(good_phens):
        keys = sorted(phen.keys())

        if VERBOSE: print "\t\t",
        for key in keys:
            if VERBOSE: print "%s: %s; " % (key, phen[key]),

        if VERBOSE: print 

        style = " "
        phen_html = "<br/>".join(["%s: <b>%s</b>" % (key, unpack(phen[key])) for key in keys])
        phen_stuff = "<td><b>%d</b> of %d</td><td>%s</td></tr>\n" % (i + 1, n_phens, phen_html)

        if i == 0: 
            start_html = '<tr style="background-color: cyan;">'
            html = start_html + generic_html + phen_stuff 
        else:
            html = "<tr><td></td><td>%s</td><td>%s</td>" % (data_path, formats) + phen_stuff
 
        rows.append(html)

    if not good_phens:
        html = generic_html + "<td>0 of 0</td><td></td></tr>\n"
        rows.append(html)

    return "\n".join(rows)


def unpack(item):
    "Utility function to convert a sequence to a single item if only contains one item."
    if type(item) in (list, tuple) and len(item) == 1:
        return item[0]
    return item


def process_obs_to_html(paths_page="http://catalogue.ceda.ac.uk/export/paths/"):
    """
    Looks up each Observation in the MOLES catalogue, matches phenomena to it 
    from ES and then writes HTML pages listing them. 
    """
    lines = urllib.urlopen(paths_page).readlines()
    lines.sort()
    n = len(lines)

    SPLIT = 100
    page_number = 1
    TEMPL = "extracted_phenomena_%02d.html"
    cfg = read_cfg()

    while lines:
        lines_to_process = lines[:SPLIT]
        lines = lines[SPLIT:]
        content = ""

        for i, line in enumerate(lines_to_process):
            if i > 50000000: 
                lines = []
                break

            data_path, ob_url = line.strip().split()
            #print "Working on: %s" % data_path

            try:
                results = fbs_api.get_dir_info(data_path)
            except:
                continue

            if len(results["formats"]) > 0:
                print "Formats in directory {} are {} and some files {}".format(data_path, results["formats"], results["sample_names"])
                #find also some file.
                #res = find_doc(cfg, data_path)
                #print res[u"hits"][0][u"_source"]


            #html = render_results(ob_url, data_path, results, i + 1, n)
            #content += "\n" + html

        #title = "File-based search review of MOLES records: %d" % page_number
        #previous_link = TEMPL % (page_number - 1)
        #next_link = TEMPL % (page_number + 1)
        #page = PAGE_TEMPLATE % vars()

        #fpath = "%s/extracted_phenomena_%02d.html" % (OUT_DIR, page_number)
        #with open(fpath, "w") as html_writer:
        #    html_writer.write(page)

        #print "Wrote: %s" % fpath
        #page_number += 1

if __name__ == "__main__":

    process_obs_to_html() 
