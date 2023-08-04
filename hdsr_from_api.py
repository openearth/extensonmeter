#Nathalie Dees
#retrieving timeseries data from waterboards from Lizard API (v4!)~
#before use, check current lizard version
#later, data will be put in a database

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
    fc = r"C:\projecten\grondwater_monitoring\ijmuiden\dbasetools\connection_local.txt"
else:
    fc = r"C:\projecten\grondwater_monitoring\ijmuiden\connection_online.txt"
session,engine = establishconnection(fc)

# %%
# the url to retrieve the data from, groundwaterstation data 

ground = "https://hdsr.lizard.net/api/v4/groundwaterstations/"
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
        #print( response['results'][i]['filters'][0]['code'])
        #creation of a metadata dict to store the data
        #loops over filters in case there are more filters
        for j in range(len(response['results'][i].get('filters'))):
            metadata= {
                'locatie.naam' : response['results'][i]['filters'][j]['code'], 
                'aquifer_confinement' :response['results'][i]['filters'][j]['aquifer_confinement'],
                'filter_bottom_level':response['results'][i]['filters'][j]['filter_bottom_level'],
                'filter_top_level':response['results'][i]['filters'][j]['filter_top_level'],
                'top_level' :response['results'][i]['filters'][j]['top_level'],
                'x' : geom["coordinates"][0],
                'y' : geom["coordinates"][1],
                'url': response['results'][i]['url']}
            ts = response['results'][i]['filters'][0]['timeseries'][0]
            timeurllist.append([ts])
            #conversion to df
            gdata.append(metadata)

            #new call to retrieve timeseries
            tsresponse = requests.get(ts).json()
            start = tsresponse['start']
            end= tsresponse['end']

            if start is not None or end is not None:
                params = {'start': start, 'end': end}
                t = requests.get(ts + 'events', params=params).json()['results']
            #only retrieving data which has a flag below four, flags are added next to the timeseries
            #this is why we first need to extract all timeseries before we can filter on flags... 
            #for flags see: https://publicwiki.deltares.nl/display/FEWSDOC/D+Time+Series+Flags
                if t[i]['flag']<5:
                    tsv.extend(t)
                    print('timeseries flag is below <5 ' + response['results'][i]['filters'][0]['code'])
                else:
                    print('flag is >5 for location: ' + response['results'][i]['filters'][0]['code'])
            timeseries = pd.DataFrame.from_dict(tsv) #check size of timeseries to see if data is returned
            df = pd.DataFrame(gdata)

        ## here a part to upload the timeseries into the DB
# %%
