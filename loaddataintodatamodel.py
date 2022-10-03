# -*- coding: utf-8 -*-
"""
Created on Tue Jul 19 12:05:14 2022

@author: hendrik_gt

#  Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2022 Deltares for Projects with a FEWS datamodel in
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

"""
import os
import pandas as pd
import requests
import time
from datetime import datetime
import configparser
import numpy as np

# third party packages
from sqlalchemy.sql.expression import update
from sqlalchemy import exc,func

# local procedures
from orm_timeseries import Base,FileSource,Location,Parameter,Unit,TimeSeries,TimeSeriesValuesAndFlags,Flags
from ts_helpders import establishconnection, read_config, loadfilesource,location,sparameter,sserieskey,sflag,dateto_integer,convertlttodate, stimestep

#functions to use to import from ftp box
from datamanagement import Sftp

# ------------------------config. making connection to ftp and databases
# TODO integrate neatly
local = True
if local:
    fc = r"C:\projecten\rws\2022\extensometer\data\connection_local.txt"
else:
    dirname =  os.path.dirname(__file__)
    fc = os.path.join(dirname,'aconnection.txt')
session,engine = establishconnection(fc)

configfile = r'C:\projecten\rws\2022\extensometer\config.txt'
cf = configparser.ConfigParser() 
cf.read(configfile)      

sftp = Sftp(
        hostname=cf.get('FTP','url'),
        username=cf.get('FTP','user'),
        password=cf.get('FTP','password'),
        port=cf.get('FTP','port'),
    )

#----------------Automatisch ophalen van de ftp box en wegschrijven op locaties in de p-schijf
#TODO uitwerken logische structuur op p schijf

# TEST
#data ophalen van 4 plekken, zijn verschillende mapjes in de ftp
rmpath = ['./cabauw','./berkenwoude', '/.bleskensgraaf','./hazerswoude']

# Connect to SFTP
sftp.connect()

#test data ophalen

#for i in sftp.listdir_attr(rmpath[0]):
    #print(i)

# test data download

lpath = r'C:\projecten\rws\2022\extensometer\data\temp'

sftp.download(rmpath,lpath)

#close the connection to SFTP    
sftp.disconnect()

# ---------------administratie parameters
# get or set parameterkey 
pkeygws = sparameter(fc,'cm','Grondwaterstand',['cm-NAP', 'centimeter tov NAP'],'Grondwaterstand')
pkeytempw = sparameter(fc,'Tw','Temperatuur water',['Celcius','Celcius'],'Temperatuur water')
pkeytempi = sparameter(fc,'Ti','Temperatuur intern',['Celcius','Celcius'],'Temperatuur intern')

#--------------ADMINISTRATIE locationtable
# administrate file with metadata, inlezen. metadata wordt handmatig bewerkt in excel file oid
sf = r'C:\projecten\rws\2022\extensometer\metadata_test.xlsx'
fskey = loadfilesource(sf,fc,'metadata')
df = pd.read_excel(sf,names=['name','diverid', 'x','y','epsgcode'])#'tubetop','depth_pb','diverid', 'cablelength'])#,'filterid'])

df['name']=df['name'].str.lower()
# find location or add to location table
# before inserting, get latest id
stmt = """SELECT max(locationkey) from {s}.{t};""".format(s='timeseries',t='location')
r = engine.execute(stmt).fetchall()[0][0]
if r is None:
    lid = 1
    df.index += lid  
else:
    lid = r
    df.index = np.arange(1, (lid+1)) 
df['locationkey']=df.index
df['filesourcekey']=fskey[0][0]

#if the location table is up to date, it will skip this part and not update to the database 
if lid != df["locationkey"].iloc[-1]:
    # store the metadata in the database
    df.to_sql('location',engine,schema='timeseries',index=None,if_exists='append')

    # update the table set the geometry for those records that have null as geom
    stmt = """update {s}.{t} set geom = st_setsrid(st_point(x,y),epsgcode) where geom is null;""".format(s='timeseries',t='location')
    engine.execute(stmt)
    stmt = """select geom, locationkey, name, diverid from {s}.{t} """.format(s='timeseries',t='location')

#rename location table to use later
metadata=df


# ----------administre timestepkey and flags
# TODO dit netjes maken
tstkey = stimestep(session,'def of timestep','a label')

flagkey = sflag(fc,'validated data','validated by waterboard')

# TODO make loop to fill all
root_dir=r'C:\projecten\rws\2022\extensometer\data'

# !! testing 
def list_files(dir):
    r = []
    for root, dirs, files in os.walk(dir):
        for name in files:
            r.append(os.path.join(root, name))
    return r

t=list_files(root_dir)

dir=r'C:\projecten\rws\2022\extensometer\data\berkenwoude\ElliTrack-21121601-93537417.txt'
name='berkenwoude'

# ------------------tijdseries in de database stoppen
# fill dataframe with timeseries and add all necessary keys to the datafram
# lees in files vanaf de p schijf die net ingelezen is 
# niet elke keer opnieuw door de p schijf lopen maar alleen de data toevoegen die net opgehaald is van de ftp box
locationkey= list(metadata.loc[metadata['name'] == name, 'locationkey']) #find locationkey based on matadata table #name is folder name? 
fskey = loadfilesource(dir,fc,'timeseries')

df=pd.read_csv(dir, sep="\t")

skeygws = sserieskey(fc, pkeygws, locationkey[0], fskey[0],timestep='nonequidistant') #pkeygws
skeytempw = sserieskey(fc, pkeytempw, locationkey[0], fskey[0],timestep='nonequidistant') #pkeytemp
skeytempi = sserieskey(fc, pkeytempi, locationkey[0], fskey[0],timestep='nonequidistant') #pkeytemp

df['Datum'] = pd.to_datetime(df['Datum']) #change to datetime format
df.rename(columns = {'Datum':'datetime'}, inplace = True) #change column name

#adding waterstand to db
dfw= df[['datetime', 'Waterstand']]
dfw.rename(columns = {'Waterstand':'scalarvalue'}, inplace = True) #change column name
dfw['timeserieskey'] = skeygws #set series key for gws en temp
dfw['flags' ] = flagkey

dfw.to_sql('timeseriesvaluesandflags',engine,index=False,if_exists='append',schema='timeseries')

#adding temperatuur water to db
dft= df[['datetime', 'Temperatuur water']]
dft.rename(columns = {'Temperatuur water':'scalarvalue'}, inplace = True) #change column name
dft['timeserieskey'] = skeytempw #set series key for gws en temp
dft['flags' ] = flagkey

dft.to_sql('timeseriesvaluesandflags',engine,index=False,if_exists='append',schema='timeseries')

#adding temperatuur intern to db
dfti= df[['datetime', 'Temperatuur intern']]
dfti.rename(columns = {'Temperatuur intern':'scalarvalue'}, inplace = True) #change column name
dfti['timeserieskey'] = skeytempi #set series key for gws en temp
dfti['flags' ] = flagkey

dfti.to_sql('timeseriesvaluesandflags',engine,index=False,if_exists='append',schema='timeseries')

