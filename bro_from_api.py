#  Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2023 Deltares
#                 Somers project with PostgreSQL/PostGIS database
#   Gerrit Hendriksen (gerrit.hendriksen@deltares.nl)
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

#Gerrit Hendriksen
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
import hydropandas as hpd


# local procedures
from orm_timeseries_bro import Base,FileSource,Location,Parameter,Unit,TimeSeries,TimeSeriesValuesAndFlags,Flags
from ts_helpders_bro import establishconnection, read_config, loadfilesource,location,sparameter,sserieskey,sflag,dateto_integer,convertlttodate, stimestep


# %% 
#----------------postgresql connection
# data is stored in PostgreSQL/PostGIS database. A connection string is needed to interact with the database. This is typically stored in
# a file.

local = False
if local:
    fc = r"C:\develop\extensometer\localhost_connection.txt"
else:
    fc = r"C:\develop\extensometer\connection_online.txt"
session,engine = establishconnection(fc)

# %%
# set parameter, timeseries and flag
flagid = sflag(fc,'goedgekeurd')
fid = loadfilesource('BRO data',fc,remark='derived with Hydropandas package')[0][0]
pid = sparameter(fc,'grondwater','grondwater',('stand','m-NAP'),'grondwater')



# %%
# First get a list of BRO id's from the table with the following requirements:
# - veenparcel = True
# - removode = 'nee'

strSql = """select bro_id,number_of_monitoring_tubes from gwmonitoring.groundwater_monitoring_well 
            where veenperceel and removed = 'nee'"""

conn = engine.connect()
res = conn.execute(strSql)
for i in res:
    bro_id = i[0]
    nr_tubes = i[1]
    for t in range(1,nr_tubes+1):
        gw_bro = hpd.GroundwaterObs.from_bro(bro_id, tube_nr=t,tmin='2010-01-01')
        if len(gw_bro) > 0:
            print('adding data from BROID',gw_bro.name)
            lid = location(fc,fid,
                        name = gw_bro.name,
                        x = gw_bro.x,
                        y = gw_bro.y,
                        filterid =int(gw_bro.tube_nr),
                        epsg=28992,
                        shortname=gw_bro.filename,
                        description='',
                        altitude_msl=gw_bro.ground_level,
                        z = gw_bro.tube_top,
                        tubetop=gw_bro.screen_top, 
                        tubebot=gw_bro.screen_bottom)
        
            sid = sserieskey(fc,pid,lid,fid,'nonequidistant')
            dfval = gw_bro.copy(deep=True)
            dfval['timeserieskey'] = sid
            dfval['flags'] = flagid
            dfval.rename(columns={'values':'scalarvalue'},inplace=True)
            dfval.index.name='datetime'
            dfval.reset_index(level=['datetime'],inplace=True)
            dfval.drop(['qualifier'],axis=1,inplace=True)
            dfval.dropna(inplace=True)
            if len(dfval) > 0:
                dfval.to_sql('timeseriesvaluesandflags', 
                            engine,if_exists='append',schema='gwmonitoring',
                            index=False, method='multi')

