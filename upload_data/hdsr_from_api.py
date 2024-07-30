#  Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2023 Deltares
#                 Somers project with PostgreSQL/PostGIS database
#   Nathalie Dees (nathalie.dees@deltares.nl)
#
#   This library is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This library is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this library.  If not, see <http://www.gnu.org/licenses/>.
#   --------------------------------------------------------------------
#
# This tool is part of <a href="http://www.OpenEarth.eu">OpenEarthTools</a>.
# OpenEarthTools is an online collaboration to share and manage data and
# programming tools in an open source, version controlled environment.
# Sign up to recieve regular updates of this function, and to contribute
# your own tools.

# Nathalie Dees
# retrieving timeseries data from waterboards from Lizard API (v4!)~
# before use, check current lizard version
# later, data will be put in a database

# %%
import os
import pandas as pd
import requests
from datetime import datetime
import numpy as np
import configparser

# third party packages
from sqlalchemy.sql.expression import update
from sqlalchemy import exc, func
from sqlalchemy.dialects import postgresql

# local procedures
from orm_timeseries_hdsr import (
    Base,
    FileSource,
    Location,
    Parameter,
    Unit,
    TimeSeries,
    TimeSeriesValuesAndFlags,
    Flags,
)
from ts_helpers_hdsr import (
    establishconnection,
    read_config,
    loadfilesource,
    location,
    sparameter,
    sserieskey,
    sflag,
    dateto_integer,
    convertlttodate,
    stimestep,
)

# ------temp paths/things for testing
path_csv = r"C:\projecten\nobv\2023\code"
# -------

# %%
# ----------------postgresql connection
# data is stored in PostgreSQL/PostGIS database. A connection string is needed to interact with the database. This is typically stored in
# a file.

local = False
if local:
    fc = r"C:\projecten\grondwater_monitoring\nobv\2023\connection_local_somers.txt"
else:
    fc = r"C:\projecten\grondwater_monitoring\nobv\2023\connection_online_qsomers.txt"
session, engine = establishconnection(fc)


# Functions
# function to find latest entry
def latest_entry(skey):
    """function to find the lastest timestep entry per skey.
    input = skey
    output = pandas df containing either none or a date"""
    stmt = """select max(datetime) from hdsr_timeseries.timeseriesvaluesandflags
        where timeserieskey={s};""".format(
        s=skey
    )
    r = engine.execute(stmt).fetchall()[0][0]
    r = pd.to_datetime(r)
    return r


configfile = r"C:\projecten\grondwater_monitoring\nobv\2023\apikey\hdsr_confiig.txt"
cf = configparser.ConfigParser()
cf.read(configfile)

# Authentication
username = "__key__"
password = cf.get("API", "apikey")  # API key

json_headers = {
    "username": username,
    "password": password,
    "Content-Type": "application/json",
}
# %%
# the url to retrieve the data from, groundwaterstation data

ground = "https://hdsr.lizard.net/api/v4/groundwaterstations/"
# creation of empty lists to fill during retrieving process
gdata = []
tsv = []
timeurllist = []

# retrieve information about' the different groundwater stations, this loops through all the pages
response = requests.get(ground, headers=json_headers).json()
groundwater = response["results"]
while response["next"]:
    response = requests.get(response["next"]).json()
    groundwater.extend(response["results"])

    # start retrieving of the seperate timeseries per groundwaterstation
    for i in range(len(response)):
        geom = response["results"][i]["geometry"]
        # creation of a metadata dict to store the data
        if response["results"][i][
            "filters"
        ]:  # looks if key 'filters'is filled, if not, it skips the entry
            for j in range(len(response["results"][i].get("filters"))):
                fskey = loadfilesource(response["results"][i]["url"], fc)

                locationkey = location(
                    fc=fc,
                    fskey=fskey[0][0],
                    name=response["results"][i]["filters"][j]["code"],
                    x=geom["coordinates"][0],
                    y=geom["coordinates"][1],
                    epsg=4326,
                    description=response["results"][i]["station_type"],
                    altitude_msl=response["results"][i]["filters"][j]["top_level"],
                    tubetop=response["results"][i]["filters"][j]["filter_top_level"],
                    tubebot=response["results"][i]["filters"][j]["filter_bottom_level"],
                )

                # here there is a call to find out if there is a timeseries entry is in the filter column
                if response["results"][i]["filters"][j][
                    "timeseries"
                ]:  # some fill in filters but not timeseries so sort for that
                    # some filter entries contain up to four timeseries entries
                    # also need to loop over all the timeseries entries
                    for k in range(
                        len(response["results"][i]["filters"][j]["timeseries"])
                    ):
                        if response["results"][i]["filters"][0]["timeseries"]:

                            ts = response["results"][i]["filters"][j]["timeseries"][k]

                            # new call to retrieve timeseries
                            tsresponse = requests.get(ts).json()

                            params = {
                                "value__isnull": False,
                                "time__gte": "2018-01-01T00:00:00Z",
                                "page_size": "100",
                            }
                            # to get the actual timeseries, we need to call the 'events' parameter
                            # therefor we need to make a new request, where we can use the parameters which we defined previouslgy
                            t = requests.get(ts + "events", params=params).json()

                            # only retrieving data which has a flag below five, flags are added next to the timeseries
                            # this is why we first need to extract all timeseries before we can filter on flags...
                            # for flags see: https://publicwiki.deltares.nl/display/FEWSDOC/D+Time+Series+Flags
                            if t["results"]:
                                while t[
                                    "next"
                                ]:  # iteration trhough all the different timeseries page, continue as long as there is a next page
                                    t = requests.get(t["next"]).json()

                                    pkeygws = sparameter(
                                        fc,
                                        tsresponse["observation_type"]["unit"],
                                        tsresponse["observation_type"]["parameter"],
                                        [
                                            tsresponse["observation_type"]["unit"],
                                            tsresponse["observation_type"][
                                                "reference_frame"
                                            ],
                                        ],  # unit
                                        tsresponse["observation_type"]["description"],
                                    )
                                    skeygws = sserieskey(
                                        fc,
                                        pkeygws,
                                        locationkey,
                                        fskey[0],
                                        timestep="nonequidistant",
                                    )

                                    df = pd.DataFrame.from_dict(t["results"])
                                    df = df[df.flag == 0]
                                    print(df.flag.size)
                                    if df.flag.size > 0:
                                        flagkey = sflag(
                                            fc, str(df.flag.values[0]), "FEWS-flag"
                                        )

                                        df["datetime"] = pd.to_datetime(df["time"])

                                        r = latest_entry(skeygws)
                                        print(r)
                                        if r != (df["datetime"].iloc[-1]).replace(
                                            tzinfo=None
                                        ):
                                            df.drop(
                                                columns=[
                                                    "validation_code",
                                                    "comment",
                                                    "time",
                                                    "last_modified",
                                                    "detection_limit",
                                                    "flag",
                                                ],
                                                inplace=True,
                                            )
                                            df = df.rename(
                                                columns={"value": "scalarvalue"}
                                            )  # change column
                                            df["timeserieskey"] = skeygws
                                            df["flags"] = flagkey
                                            df.to_sql(
                                                "timeseriesvaluesandflags",
                                                engine,
                                                index=False,
                                                if_exists="append",
                                                schema="hdsr_timeseries",
                                            )
                                            print(
                                                "updated: ",
                                                response["results"][i]["filters"][j][
                                                    "code"
                                                ],
                                            )
# %%
