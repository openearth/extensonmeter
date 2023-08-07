#  Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2023 Deltares for Projects with a FEWS datamodel in
#                 PostgreSQL/PostGIS database used various project
#   Gerrit Hendriksen@deltares.nl
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

# %%
import os
import pandas as pd
import requests
from datetime import datetime
import numpy as np

# third party packages
from sqlalchemy.sql.expression import update
from sqlalchemy import exc,func
from sqlalchemy.dialects import postgresql

# local procedures
from orm_timeseries import Base,FileSource,Location,Parameter,Unit,TimeSeries,TimeSeriesValuesAndFlags,Flags
from ts_helpders import establishconnection, read_config, loadfilesource,location,sparameter,sserieskey,sflag,dateto_integer,convertlttodate, stimestep

#------temp paths/things for testing
path_csv = r'C:\projecten\nobv\2023\code'
#-------

# %% 
#----------------postgresql connection
# data is stored in PostgreSQL/PostGIS database. A connection string is needed to interact with the database. This is typically stored in
# a file.

local = True
if local:
    fc = r"C:\projecten\grondwater_monitoring\nobv\2023\connection_local_somers.txt"
else:
    fc = r"C:\projecten\grondwater_monitoring\ijmuiden\connection_online.txt"
session,engine = establishconnection(fc)

#Functions
#function to find latest entry 
def latest_entry(skey):
    """function to find the lastest timestep entry per skey. 
    input = skey
    output = pandas df containing either none or a date"""
    stmt="""select max(datetime) from timeseries.timeseriesvaluesandflags
        where timeserieskey={s};""".format(s=skey)
    r = engine.execute(stmt).fetchall()[0][0]
    r=pd.to_datetime(r) 
    return r 

#funcion to find if location was already in location table
#find location or add to location table
# %% Retrieving data from API and putting in database
# the url to retrieve the data from, groundwaterstation data 
ground = "https://hhnk.lizard.net/api/v4/groundwaterstations/"
#creation of empty lists to fill during retrieving process
gdata = []
tsv=[]
timeurllist= []

#retrieve information about the different groundwater stations, this loops through all the pages
response = requests.get(ground).json()
groundwater = response['results']
while response["next"]:
    response = requests.get(response["next"]).json()
    groundwater.extend(response["results"])

#start retrieving of the seperate timeseries per groundwaterstation
    for i in range(len(response)):
        geom = response['results'][i]['geometry']
        #creation of a metadata dict to store the data
        if response['results'][i].get("filters") is not None:  #looks if key 'filters'is filled, if not, it skips the entry
            for j in range(len(response['results'][i].get('filters'))):
                if response['results'][i]['filters'][j].get('timeseries') == []: #some fill in filters but not timeseries so sort for that
                    continue
                else:
                    #some filter entries contain up to four timeseries entry, therefore we
                    #also need to loop over all the timeseries entries
                    for k in range(len(response['results'][i]['filters'][j]['timeseries'])):
                        ts = response['results'][i]['filters'][j]['timeseries'][k]
                        timeurllist.append([ts])

                        #new call to retrieve timeseries
                        tsresponse = requests.get(ts).json()
                        start = tsresponse['start']
                        end= tsresponse['end']

                        if start is not None or end is not None:
                            params = {'start': start, 'end': end}
                            t = requests.get(ts + 'events', params=params).json()['results']
                        #only retrieving data which has a flag below five, flags are added next to the timeseries
                        #this is why we first need to extract all timeseries before we can filter on flags... 
                        #for flags see: https://publicwiki.deltares.nl/display/FEWSDOC/D+Time+Series+Flags
                            if t[i]['flag']<5:
                                fskey = loadfilesource(response['results'][i]['url'],fc)
                                locationkey=location(fc=fc, 
                                                    fskey=fskey[0][0],
                                                    name=response['results'][i]['name'],
                                                    x=geom["coordinates"][0],
                                                    y=geom["coordinates"][1],
                                                    epsg=4326,
                                                    diverid=response['results'][i]['filters'][j]['code'],
                                                    altitude_msl=response['results'][i]['filters'][j]['top_level'],
                                                    tubebot=response['results'][i]['filters'][j]['filter_bottom_level'],
                                                    tubetop=response['results'][i]['filters'][j]['filter_top_level'])

                                tsv.extend(t)
                               # print('timeseries flag is below <5 ' + response['results'][i]['filters'][0]['code'])
                            else:
                                continue
                                #print('flag is >5 for location: ' + response['results'][i]['filters'][0]['code'])
                    timeseries = pd.DataFrame.from_dict(tsv) #check size of timeseries to see if data is returned
                    #dft = pd.DataFrame(gdata)

# %%