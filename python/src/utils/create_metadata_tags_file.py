#!/usr/bin/env python

"""
create_metadata_tags_file.py
============================

Reads input files (or a string in the example given) and creates a 
"metadata tags" file written to standard output.

"""

import json

data = """long_name     var_id  units   comment
Maximum temperature     TMAX    degrees_c
Minimum temperature     TMIN    degrees_c
Daily mean temperature  TEMP5   degrees_c       Temperature at 1.5m elevation averaged over the daily cycle (i.e. over all the five minute time-steps)
Total precipitation rate        PREC    mm/mth
Snowfall rate   SNOW    mm/day  Rate of accumulation of equivalent water (i.e. all snow melted)
Wind speed      WIND    m/s     Wind speed at 10m elevation
Relative humidity       RHUM    %
Total cloud in longwave radiation (fraction)    TCLW    %       Fractional cloud cover
Net surface longwave flux       NSLW    W/m     L(up) - L(down) (usually negative, i.e. upward)
Net surface shortwave flux      NSSW    W/m     DSWF multiplied by (1 - ground albedo)
Total downward surface shortwave flux   DSWF    W/m     Direct + diffuse solar radiation
Soil moisture content10 SMOI    mm      Moisture in the root zone (i.e. available for evapotranspiration)
Mean sea level pressure MSLP    hpa
Surface latent heat flux        SLHF    W/m     Energy lost by the surface due to evaporation of water from the soil of the vegetation canopy
Specific humidity       SPHU    g/kg
Inter-annual variability: temperature   IAVT    No
Inter-annual variability: precipitation IAVP    No      """.split("\n")

def get_data(lines=data):
    phens = []
    keys = lines[0].split("\t")

    for line in lines[1:]:
        values = line.split("\t")
        d = dict([(key, value) for key,value in zip(keys, values) if (value != "") and not (key == "units" and value == "No")])
        phens.append(d)

    return phens


def create_json():
    d = {
    "directory": "/badc/ukcip02/data/50km_resolution",
    "phenomena": get_data(),
    "geospatial": [-11.0, 61, 3, 49],
    "time": ["1961-01-01T00:00:00", "2100-12-31T23:59:59"],
    "exclusion_regexes": [".*pdf"]
    }

    json_file = "metadata_tags.json"
    print "Writing file: %s" % json_file
    with open(json_file, "w") as writer:
        json.dump(d, writer)

if __name__ == "__main__":

    create_json()
