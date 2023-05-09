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
# %%
import os
import pandas as pd
import requests
import time
from datetime import datetime
import configparser
import numpy as np

# setup connection with sftp box
import pysftp
from urllib.parse import urlparse
import shutil

# third party packages
from sqlalchemy.sql.expression import update
from sqlalchemy import exc,func

# local procedures
from orm_timeseries import Base,FileSource,Location,Parameter,Unit,TimeSeries,TimeSeriesValuesAndFlags,Flags
from ts_helpders import establishconnection, read_config, loadfilesource,location,sparameter,sserieskey,sflag,dateto_integer,convertlttodate, stimestep

#from ext_loaddataintodatamodel import metadata_location
from sftp_tools import Sftp

# ------------------------config. making connection to ftp and databases
# TODO integrate neatly
local = False
if local:
    fc = r"C:\projecten\nobv\2023\connection_local.txt"
else:
    fc = r"C:\projecten\nobv\2023\connection_online.txt"

session,engine = establishconnection(fc)
#%%
# ---------------administratie parameters
# get or set parameterkey 
pkeygws = sparameter(fc,'cm','Grondwaterstand',['cm-NAP', 'centimeter tov NAP'],'Grondwaterstand')
pkeytempw = sparameter(fc,'Tw','Temperatuur water',['Celcius','Celcius'],'Temperatuur water')
pkeytempi = sparameter(fc,'Ti','Temperatuur intern',['Celcius','Celcius'],'Temperatuur intern')

# ----------administre timestepkey and flags
# TODO dit netjes maken
#TODO flags toevoegen voor of het ellitrack nobv data, waterschaps data ect ix
tstkey = stimestep(session,'1 hour','hourly data')

flagkey = sflag(fc,'Ellitrack data','unvalidated')

# %%
#--------------ADMINISTRATIE locationtable
# administrate file with metadata
sf = r'P:\11207812-somers-ontwikkeling\database_grondwater\overzicht_nobv_type1.xlsx'
#sf=r'C:\projecten\grondwater_monitoring\ijmuiden\data\peilbuizen_metadata.xlsx'
fskey = loadfilesource(sf,fc,'metadata')
df = pd.read_excel(sf)
df = df.set_axis(['name','diverid','description','category','x','y','tubetop','tubebot','altitude_msl','distance_w','distance_p', 'registered'], axis=1)
df = df.drop(columns=['category','distance_w','distance_p', 'registered'])
df['epsgcode'] = 28992
df['diverid'] = df['diverid'].astype('Int64') #convert to Int64 to set data type as int array which can contain null values
#find location or add to location table
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

#if the location table is up to date, it will skip this part. 
if lid != df["locationkey"].iloc[-1]:
    # store the metadata in the database
    df.to_sql('location',engine,schema='timeseries',index=None,if_exists='append')

    # update the table set the geometry for those records that have null as geom
    stmt = """update {s}.{t} set geom = st_setsrid(st_point(x,y),epsgcode) where geom is null;""".format(s='timeseries',t='location')
    engine.execute(stmt)
    #stmt = """select geom, locationkey, name, shortname, filterid, filterdepth, tubetop, tubebot from {s}.{t} """.format(s='timeseries',t='location')

# %%
#READ FROM SFTP BOX
#configfile = r'D:\projecten\datamanagement\Nederland\BodembewegingNL\tools\config.txt'
configfile = r'C:\projecten\rws\2022\extensometer\config.txt'
cf = configparser.ConfigParser() 
cf.read(configfile)      

sftp = Sftp(
        hostname=cf.get('FTP','url'),
        username=cf.get('FTP','user'),
        password=cf.get('FTP','password'),
        port=cf.get('FTP','port'),
    )
    
# list items + paths for input + outputs
#manually make a list with the folder locations from ellitrack

#Nog toe te voegen aan de database
#lstdir = ['cabauw','bleskensgraaf','berkenwoude', 'gouderak', 'vegelinsoord', 'hazerswoude', 'vlist','zegveld']

#tot zo ver de enige waarvan we zowel metadata als gegevens in een ellitrack portaal hebben
#lstdir =  ['rouveen']
lstdir =  ['assendelft','aldeboarn', 'rouveen']

lpath = 'C:\\projecten\\nobv\\2023\\archief_lokaal'
ppath= 'P:\\11207812-somers-uitvoering\\database_grondwater\\archief\\'

#select locationkeys and diverids from database
stmt = """SELECT locationkey, diverid from {s}.{t};""".format(s='timeseries',t='location')
r=engine.execute(stmt).fetchall()
metadata=pd.DataFrame(r)

# %%
def latest_entry(skey):
    """function to find the lastest timestep entry per skey. 
    input = skey
    output = pandas df containing either none or a date"""
    stmt="""select max(datetime) from timeseries.timeseriesvaluesandflags
        where timeserieskey={s};""".format(s=skey)
    r = engine.execute(stmt).fetchall()[0][0]
    r=pd.to_datetime(r) 
    return r 

#%%
# Connect to SFTP
sftp.connect()

#sftp.download(rmpath,lpath)
for dir in lstdir:
    rmpath = './{rm}/'.format(rm=dir) #remote path is link to the sftp and name of the folder 
    sftp.listdir_attr(rmpath)
    for i in sftp.listdir_attr(rmpath):
        filepath=(lpath+'\\{rm}\\').format(rm=dir)+i.filename #assigning the right filename to file
        sftp.download(rmpath+i.filename,filepath) #download from ftp to local location

        name=i.filename.split('-')[1]
        #skipping files which are not correct
        if name == "21070208" or name == "22081306" or name =='22081307' or name =='20040803' or name =='22072101': #skip these ellitrack number as it is not correctly in metadata file yet 
            continue
        
        locationkey= list(metadata.loc[metadata['diverid'] == name, 'locationkey']) #find locationkey based on matadata table #name is folder name? 
        print(name,locationkey)

        #administrate final store location of file source
        fskey = loadfilesource((os.path.join(ppath,dir)+'\\'+i.filename),fc,'zoetwaterstijgghoogte')

        # if dir does not exist in root then make dir
        if not os.path.exists(ppath+dir):
            os.mkdir(ppath+dir)
            
        #select between .txt files and .xml files
        if filepath.endswith('.txt'):
            df=pd.read_csv(filepath, sep="\t")
            df['Datum'] = pd.to_datetime(df['Datum']) #change to datetime format
        elif filepath.endswith('.xml'):
            if os.stat(filepath).st_size == 0: #skipping empty files in ellitrack
                continue
            
           # df=pd.read_xml(filepath,xpath="//trackerinfo")
            dfdates = pd.read_xml(filepath,xpath=".//measurement")
            dfdata = pd.read_xml(filepath,xpath=".//input")

            #extract data, reset indices, combine again
            id1=dfdata[dfdata['id'] == 1].reset_index(drop=True)
            id2=dfdata[dfdata['id'] == 2].reset_index(drop=True)
            id3=dfdata[dfdata['id'] == 3].reset_index(drop=True)
            dfd=pd.concat([id1,id2,id3])

            dfvalues=dfd.pivot(columns='id', values='data')
            df=dfdates.join(dfvalues).drop(columns=['input'])
            df.columns=['Datum','Waterstand','Temperatuur water', 'Temperatuur intern']
            df['Datum']=pd.to_datetime(df['Datum'])  #change to datetime format
            #remove UTC timestamp from timeseries only in XMLS
            #turned off for now, updating database in local timezone
            #df['Datum'] = df['Datum'].dt.tz_convert(None)

        skeygws = sserieskey(fc, pkeygws, locationkey[0], fskey[0],timestep='nonequidistant') #pkeygws
        skeytempw = sserieskey(fc, pkeytempw, locationkey[0], fskey[0],timestep='nonequidistant') #pkeytemp
        skeytempi = sserieskey(fc, pkeytempi, locationkey[0], fskey[0],timestep='nonequidistant') #pkeytemp

        df.rename(columns = {'Datum':'datetime'}, inplace = True) #change column name
        
        #TODO retrieving from the DB is a timeseries without timezone! this is not correct yet
        r=latest_entry(skeygws)

        tempw=latest_entry(skeytempw)
        tempi=latest_entry(skeytempi)

        dfw= df[['datetime', 'Waterstand']]
        dft= df[['datetime', 'Temperatuur water']]
        dfti= df[['datetime', 'Temperatuur intern']]

        #adding waterstand to db
        if r!=(dfw['datetime'].iloc[-1]).replace(tzinfo=None):
            dfw= df[['datetime', 'Waterstand']]
            dfw=dfw.rename(columns = {'Waterstand':'scalarvalue'}) #change column name
            dfw['timeserieskey'] = skeygws #set series key for gws en temp
            dfw['flags' ] = flagkey

            dfw.to_sql('timeseriesvaluesandflags',engine,index=False,if_exists='append',schema='timeseries')
        else:
            print('not updating timeseries gws:', name)

            #adding temperatuur water to db
        if tempw!=(dft['datetime'].iloc[-1]).replace(tzinfo=None):
            dft= df[['datetime', 'Temperatuur water']]
            dft=dft.rename(columns = {'Temperatuur water':'scalarvalue'}) #change column name
            dft['timeserieskey'] = skeytempw #set series key for gws en temp
            dft['flags' ] = flagkey

            dft.to_sql('timeseriesvaluesandflags',engine,index=False,if_exists='append',schema='timeseries')
        else:
            print('not updating timeseries tempw:', name)

            #adding temperatuur intern to db
        if tempi!=(dft['datetime'].iloc[-1]).replace(tzinfo=None):
            dfti= df[['datetime', 'Temperatuur intern']]
            dfti=dfti.rename(columns = {'Temperatuur intern':'scalarvalue'}) #change column name
            dfti['timeserieskey'] = skeytempi #set series key for gws en temp
            dfti['flags' ] = flagkey

            dfti.to_sql('timeseriesvaluesandflags',engine,index=False,if_exists='append',schema='timeseries')
        else:
            print('not updating timeseries tempi:', name)   

        #na succsevol inladen in de database data naar de p verplaatsen,
        shutil.move(filepath, (os.path.join(ppath,dir)+'\\'+i.filename)) #write to p drive
        
        # na succesvol inladen in de database data van de ftp verwijderen
        sftp.remove_file(rmpath+i.filename) #delete from ftp


#close the connection to SFTP
sftp.disconnect()

# %%
