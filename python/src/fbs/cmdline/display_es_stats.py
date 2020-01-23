'''
Created on 24 May 2016

@author: kleanthis
'''


import matplotlib.pyplot as plt
import numpy as np
import sys
import time
from elasticsearch import Elasticsearch
import datetime
import fbs.proc.common_util.util as util
import os

def read_cfg():

    c_dir     = os.path.dirname(__file__)
    conf_path = os.path.join(c_dir, "../../../config/ceda_fbs.ini")
    config    = util.cfg_read(conf_path)

    return config

def plot(x, y):

    # The X axis can just be numbered 0,1,2,3...
    x_axis = np.arange(len(x))
    y_range = 250*(10**6)

    plt.bar(x_axis, y)

    plt.xticks(x_axis + 0.5, x, rotation=25, fontsize=8)
    plt.ylim([0, y_range])


    fig = plt.gcf()
    fig.canvas.set_window_title("Results.")

    plt.title("File scanning.", fontsize=18)
    plt.ylabel("Files.", fontsize=18)
    plt.xlabel("Time.", fontsize=18)
    plt.show()


def display_stats():

    cfg = read_cfg()

    filename =  os.path.join(cfg["core"]["log-path"], "fbs-stats.txt")

    file_contents = util.read_file_into_list(filename)

    x_axis = []
    y_axis = []
    for item in file_contents:
        values = item.split(",")
        x_axis.append(values[1])
        y_axis.append(int(values[3].rstrip()))

    plot(x_axis, y_axis)


def main():

    start = datetime.datetime.now()
    print( "Script started at: %s" %(str(start)))

    display_stats()

    end = datetime.datetime.now()
    print( "Script ended at : %s it ran for : %s" %(str(end), str(end - start)))

if __name__ == "__main__":
    main()
