#  Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2023 Deltares for RWS Waterinfo Extra
#   Gerrit.Hendriksen@deltares.nl
#   Nathalie.Dees@deltares.nl
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
Importeren van handmatige invoer in de database, uniforme input per waterschap
"""
# %%
import os
from datetime import time
import pandas as pd
import configparser
import glob
import numpy as np

# third party packages
from sqlalchemy.sql.expression import update
from sqlalchemy import exc,func
from sqlalchemy.dialects import postgresql

# local procedures
from orm_timeseries_waterschappen import Base,FileSource,Location,Parameter,Unit,TimeSeries,TimeSeriesValuesAndFlags,Flags
from ts_helpders_waterschappen import establishconnection, read_config, loadfilesource,location,sparameter,sserieskey,sflag,dateto_integer,convertlttodate, stimestep

def read_config(af):
    # Default config file (relative path, does not work on production, weird)
    # Parse and load
    cf = configparser.ConfigParser()
    cf.read(af)
    return cf

# set reference to config file
local = True
if local:
    fc = r"C:\projecten\grondwater_monitoring\nobv\2023\connection_local_somers.txt"
else:
    fc = r"C:\develop\rwsdatatools\config_chloride_rpa.txt"
session,engine = establishconnection(fc)

#Functions
#function to find latest entry 
def latest_entry(skey):
    """function to find the lastest timestep entry per skey. 
    input = skey
    output = pandas df containing either none or a date"""
    stmt="""select max(datetime) from waterschappen_timeseries.timeseriesvaluesandflags
        where timeserieskey={s};""".format(s=skey)
    r = engine.execute(stmt).fetchall()[0][0]
    r=pd.to_datetime(r) 
    return r 

#first push to the location table 
#db is qsomers, schema is hand_timeseries
#onderbrengen in 1 schema van alle handmatige uitvoer data 
# if a file is (Samenvattende_tabel) it is the metadata file and needs to be stored in the location table

#assigning parameters, either grondwaterstand or slootwaterpeil
#zoetwaterstijghoogtes
pkeygwm =sparameter(fc,'GWM', 'Grondwatermeetpunt',['m-NAP','meter NAP'],'Grondwatermeetpunt')
pkeyswm = sparameter(fc,'SWM','Slootwatermeetpunt',['m-NAP','meter NAP'],'Slootwatermeetpunt')

tstkeye = stimestep(session,'nonequidistant','')

flagkeygwm=sflag(fc,'Grondwatermeetpuntt-ruwe data', 'Grondwatermeetpunt-ruwe data')
flagkeyswm=sflag(fc,'Slootwatermeetpunt-ruwe data', 'Slootwatermeetpunt-ruwe data')

root = r'P:\11207812-somers-ontwikkeling\database_grondwater\handmatige_uitvraag_bestanden'

#location table
subdir = []
for root,subdirs,files in os.walk(root):    
    for file in files:
        if file == "SAMENVATTENDE_TABEL.txt":
            df = pd.read_csv(os.path.join(root, file), delimiter=';')
            df = df.drop(columns = ['lat', 'lon', 'attributes', 'description'])
            df = df.rename(columns={'locationId': 'name', 
                                    'TYPE': 'description',
                                    'shortName': 'shortname'})
            df['epsgcode'] = 28992
            sf = os.path.join(root, file)
            ws = os.path.join(root, file).split("\\")[-3] #find the waterschap
            fskey = loadfilesource(sf,fc, str(ws) +'_metadata')
            df['locationkey']=df.index
            df['filesourcekey']=fskey[0][0] 

            metadata=df

            # df.to_sql('location',engine,schema='waterschappen_timeseries',index=None,if_exists='append')

            # update the table set the geometry for those records that have null as geom
            # stmt = """update {s}.{t} set geom = st_setsrid(st_point(x,y),epsgcode) where geom is null;""".format(s='waterschappen_timeseries',t='location')
            # engine.execute(stmt)
        
        elif file == 'SWM_Meetlocatie_OW000001.txt':
            name=os.path.splitext(file)[0].split("_", 1)[1]
            data=file.split("_")[0] #find in name it is GWM or SWM

            fskey = loadfilesource(os.path.join(root, file),fc,str(ws) + str(type))
            header = pd.read_csv(os.path.join(root, file), sep=";", nrows=5) 
            cols = [header.iloc[3,0] , header.iloc[4,0]]
            dfx = pd.read_csv(os.path.join(root, file), delimiter=';', skiprows=6, header = None, names = cols)

            locationkey= list(metadata.loc[metadata['name'] == name, 'locationkey'])

            if data == 'SWM':
                skeyz = sserieskey(fc, pkeyswm, locationkey[0], fskey[0],timestep='nonequidistant')
                flag = flagkeyswm
            elif data == 'GWM':
                skeyz = sserieskey(fc, pkeygwm, locationkey[0], fskey[0],timestep='nonequidistant')
                flag = flagkeygwm
            else:
                print('Different serieskey and / or flagkey needed')

            dfx.rename(columns = {'> datumtijd (dd-mm-yyyy HH:MM)':'datetime',
                                  '> slootwaterstand (m NAP)':'scalarvalue',}, inplace = True)
            dfx['datetime'] = pd.to_datetime(dfx['datetime']) 
            dfx=dfx.dropna()
            dfx['timeserieskey'] = skeyz 
            dfx['flags' ] = flag

            dfx.to_sql('timeseriesvaluesandflags',engine,index=False,if_exists='append',schema='waterschappen_timeseries')
            # Set the header for the DataFrame
         
            #needs to be linked to location key
            #assign flag (ruwe data)
            #plot een aantal random tijdseries en sla op

        
    
# %%
