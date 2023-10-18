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
import configparser

# third party packages
from sqlalchemy.sql.expression import update
from sqlalchemy import exc,func
from sqlalchemy.dialects import postgresql

# local procedures
from orm_timeseries_hdsr import Base,FileSource,Location,Parameter,Unit,TimeSeries,TimeSeriesValuesAndFlags,Flags
from ts_helpders import establishconnection, read_config, loadfilesource,location,sparameter,sserieskey,sflag,dateto_integer,convertlttodate, stimestep

#------temp paths/things for testing
path_csv = r'C:\projecten\nobv\2023\code'
#-------

# %% 
#----------------postgresql connection
# data is stored in PostgreSQL/PostGIS database. A connection string is needed to interact with the database. This is typically stored in
# a file.

local = False
if local:
    fc = r"C:\projecten\grondwater_monitoring\nobv\2023\connection_local_somers.txt"
else:
    fc = r"C:\projecten\grondwater_monitoring\nobv\2023\connection_online_qsomers.txt"
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

# %%
configfile = r'C:\projecten\grondwater_monitoring\nobv\2023\apikey\hdsr_confiig.txt'
cf = configparser.ConfigParser() 
cf.read(configfile)      

# Authentication
username = '__key__'
password = cf.get('API','apikey')    # API key

json_headers = {
            "username": username,
            "password": password,
            "Content-Type": "application/json",
        }
# %%
# the url to retrieve the data from, groundwaterstation data 

ground = "https://hdsr.lizard.net/api/v4/measuringstations/"
#creation of empty lists to fill during retrieving process
gdata = []
tsv=[]
timeurllist= []

#retrieve information about' the different groundwater stations, this loops through all the pages
response = requests.get(ground, headers=json_headers).json()
groundwater = response['results']
while response["next"]:
    response = requests.get(response["next"]).json()
    groundwater.extend(response["results"])

#start retrieving of the seperate timeseries per groundwaterstation
    for i in range(len(response)):
        geom = response['results'][i]['geometry']

        #creation of a metadata dict to store the data
        fskey = loadfilesource(response['results'][i]['url'],fc)
        locationkey=location(fc=fc, 
                            fskey=fskey[0][0],
                            name=response['results'][i]['name'],
                            x=geom["coordinates"][0],
                            y=geom["coordinates"][1],
                            epsg=4326,
                            description=response['results'][i]['station_type']
                            )

        #conversion to df
        #gdata.append(metadata)
        #df = pd.DataFrame(gdata

        #does this part when the response is not empty
        if response['results'][i]['timeseries']:
            ts = response['results'][i]['timeseries'][0]

            #new call to retrieve timeseries
            tsresponse = requests.get(ts).json()
            params={'value__isnull': False, 
                    'time__gte':'2013-01-01T00:00:00Z'
                    }
            t = requests.get(ts + 'events', params=params,headers=json_headers).json()['results']
            #only retrieving data which has a flag below four, flags are added next to the timeseries
            #this is why we first need to extract all timeseries before we can filter on flags... 
            #for flags see: https://publicwiki.deltares.nl/display/FEWSDOC/D+Time+Series+Flags
            if t: 
                if t[i]['flag']<5:
                    pkeygws = sparameter(fc,
                                        tsresponse['observation_type']['unit'],
                                        tsresponse['observation_type']['parameter'],
                                        [tsresponse['observation_type']['unit'], tsresponse['observation_type']['reference_frame']], #unit
                                        tsresponse['observation_type']['description']
                                        )
                    flagkey = sflag(fc,
                                    str(t[i]['flag']),
                                    'FEWS-flag'
                                    )
                    
                    skeygws = sserieskey(fc, 
                                        pkeygws, 
                                        locationkey, 
                                        fskey[0],
                                        timestep='nonequidistant'
                                        )
                    df=pd.DataFrame.from_dict(t)

                    df['datetime']=pd.to_datetime(df['time'])

                    r=latest_entry(skeygws)
                    if r!=(df['datetime'].iloc[-1]).replace(tzinfo=None):
                        try:
                            df.drop(columns=['validation_code', 'comment', 'time', 'last_modified','detection_limit', 'flag'],inplace=True)
                            df=df.rename(columns = {'value':'scalarvalue'}) #change column
                            df['timeserieskey'] = skeygws
                            df['flags'] = flagkey
                            df.to_sql('timeseriesvaluesandflags',engine,index=False,if_exists='append',schema='hdsrtimeseries')
                        except:
                            continue 


        ## here a part to upload the timeseries into the DB
# %%
""" # %%
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
                'url': response['results'][i]['url'],
                'station_type' : response['results'][i]['s']}
            ts = response['results'][i]['filters'][0]['timeseries'][0]
            timeurllist.append([ts])
            #conversion to df
            gdata.append(metadata)

            #new call to retrieve timeseries
            tsresponse = requests.get(ts).json()
            params={'value__isnull': False}

            #if start is not None or end is not None:
            #params = {'start': start, 'end': end}
            t = requests.get(ts + 'events', params=params).json()['results']
            if t:
                print(t)
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

        ## here a part to upload the timeseries into the DB """