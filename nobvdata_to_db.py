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

 THIS IS THE PROCESSED DATA, PROCESSED BY DELTARES BEFORE, THEN UPLOADING

 """

# %%
import os
from datetime import time
import pandas as pd
import configparser
import glob
import numpy as np
import matplotlib.pyplot as plt

# third party packages
from sqlalchemy.sql.expression import update
from sqlalchemy import exc,func
from sqlalchemy.dialects import postgresql

# local procedures
from orm_timeseries_nobv import Base,FileSource,Location,Parameter,Unit,TimeSeries,TimeSeriesValuesAndFlags,Flags
from ts_helpders_nobv import establishconnection, read_config, loadfilesource,location,sparameter,sserieskey,sflag,dateto_integer,convertlttodate, stimestep


def read_config(af):
    # Default config file (relative path, does not work on production, weird)
    # Parse and load
    cf = configparser.ConfigParser()
    cf.read(af)
    return cf

def latest_entry(skey):
    """function to find the lastest timestep entry per skey. 
    input = skey
    output = pandas df containing either none or a date"""
    stmt="""select max(datetime) from nobv_timeseries.timeseriesvaluesandflags
        where timeserieskey={s};""".format(s=skey)
    r = engine.execute(stmt).fetchall()[0][0]
    r=pd.to_datetime(r) 
    return r 

prefixes = [">", "#", "*", "$"]
dntusecol = ["mex", "chflysi", "mexb"]


def skiprows(fname):
    """function that gathers columnnames and determines the header size in records

    Args:
        fname (string): full pathname to a file that will be checked for columnnames and the size of the header

    Returns:
        skiprows (integer): integer indicating the size of the header in number of records to skip
        columnnames (list of strings): list of strings that represent columnames
        xycols (list of strings): list of strings that inidicate xy coords columns
    """
    columnnames = []
    xycols = []
    skiprows = 0
    datum = None
    with open(fname, "r+") as f:
        for line in f:
            if line.startswith(tuple(prefixes)):
                if line.startswith("*"):
                    datum = line.replace("*", "").strip()
                if line.startswith(">"):
                    colname = line.replace("> ", ">").split(" ")[0].split(">")[1]
                    # print(colname)
                    if "x" in colname.lower():
                        if colname.lower() not in dntusecol:
                            xycols.append("x")
                            columnnames.append("x")
                        else:
                            columnnames.append(colname.replace("\n", ""))
                    elif "y" in colname.lower():
                        if colname.lower() not in dntusecol:
                            xycols.append("y")
                            columnnames.append("y")
                        else:
                            columnnames.append(colname.replace("\n", ""))
                    elif "tijd" in colname.lower():
                        xycols.append(colname.replace("\n", "").lower())
                        columnnames.append(colname.replace("\n", ""))
                    elif "fout" in colname.lower():
                        columnnames.append(line.replace("\n", "").split(" ")[2] + "f")
                    else:
                        columnnames.append(colname.replace("\n", ""))
                skiprows = skiprows + 1
            else:
                break
    return skiprows, columnnames, xycols, datum

def extract_info_from_text_file(filename):
    """
    Reads a text file, extracts information from lines starting with "#",
    and returns a pandas DataFrame.

    Args:
        filename (str): Path to the text file.

    Returns:
        pd.DataFrame: DataFrame containing extracted information.
    """
    with open(filename, "r") as file:
        content = file.readlines()

    # Filter lines starting with "#"
    filtered_lines = [line.strip() for line in content if line.startswith("#")]

    # Create a dictionary to store key-value pairs
    data = {}
    for line in filtered_lines:
        key, value = line[1:].split(":")
        data[key.strip()] = value.strip()

    # Create a DataFrame
    df = pd.DataFrame([data])
    return df


# set reference to config file
local = True
if local:
    fc = r"C:\projecten\grondwater_monitoring\nobv\2023\connection_local_somers.txt"
else:
    fc = r"C:\projecten\grondwater_monitoring\nobv\2023\connection_online_qsomers.txt"
session,engine = establishconnection(fc)

root = r'P:\11207812-somers-ontwikkeling\database_grondwater\handmatige_uitvraag_bestanden\NOBV'

#assigning parameters, either grondwaterstand or slootwaterpeil
#zoetwaterstijghoogtes
pkeygwm =sparameter(fc,'GWM', 'Grondwatermeetpunt',['m-NAP','meter NAP'],'Grondwatermeetpunt')
pkeyswm = sparameter(fc,'SWM','Slootwatermeetpunt',['m-NAP','meter NAP'],'Slootwatermeetpunt')

tstkeye = stimestep(session,'nonequidistant','')

flagkeygwm=sflag(fc,'Grondwatermeetpuntt-ruwe data', 'Grondwatermeetpunt-ruwe data')
flagkeyswm=sflag(fc,'Slootwatermeetpunt-ruwe data', 'Slootwatermeetpunt-ruwe data')

# %%
# Example usage:
cols_loctable=['naam_meetpunt', 'x-coor', 'y-coor','top filter (m-mv)','onderkant filter (m-mv)','maaiveld (m NAP)']
cols_metatable=['slootafstand (m)', 'zomer streefpeil (m NAP)',
       'winter streefpeil (m NAP)', 'greppel aanwezig (ja/nee)',
       'drains aanwezig (ja/nee)', 'WIS aanwezig (ja/nee)',
       'greppelafstand (m)', 'greppeldiepte (m-mv)', 'drainafstand (m)',
       'draindiepte (m-mv)', 'WIS afstand (m)', 'WIS diepte (m-mv)']

new_loctabel = ['name', 'x', 'y', 'tubetop', 'tubebot', 'altitude_msl']

timeseries = ['datetime','scalarvalue']
# filename = r'P:\11207812-somers-ontwikkeling\database_grondwater\handmatige_uitvraag_bestanden\NOBV\GWM_ASD_MP_2.txt'
# result_df = extract_info_from_text_file(filename)

# metadata = pd.read_csv(r'P:\11207812-somers-ontwikkeling\somers_v2_development\data\2-interim\pp2d_phreatic_head\metadata\master_metadata.csv')                                      
# %%

for root,subdirs,files in os.walk(root):    
    for count, file in enumerate(files):
        if file.lower().endswith(".txt"):
            name=os.path.basename(file).split("_", 1)[1].rsplit('.',1)[0]
            data=os.path.basename(file).split("_", 1)[0] #find in name it is GWM or SWM
            nrrows, colnames, xycols, datum = skiprows(os.path.join(root,file))
            
            locationkey=count
            fskey = loadfilesource(os.path.join(root,file),fc,f"{name}_{data}")
            # need to update part in the location table and another part in the location_metadata
            
            dfx = pd.read_csv(os.path.join(root,file), delimiter=';', skiprows=nrrows, header = None, names = colnames)
            # print(dfx)
            if data == 'SWM':
                print('for laterrr')
                # skeyz = sserieskey(fc, pkeyswm, locationkey[0], fskey[0],timestep='nonequidistant')
                # flag = flagkeyswm
            elif data == 'GWM':
                df= extract_info_from_text_file(os.path.join(root,file))
                locationtable = df[cols_loctable]
                locationtable.columns = new_loctabel
                #add epsg and add locationkey
                locationtable['locationkey'] = count
                locationtable['epsgcode'] = 28992
                locationtable['filesourcekey']=fskey[0][0] 

                # locationtable.to_sql('location',engine,schema='nobv_timeseries',index=None,if_exists='append')
                # stmt = """update {s}.{t} set geom = st_setsrid(st_point(x,y),epsgcode) where geom is null;""".format(s='nobv_timeseries',t='location')
                # engine.execute(stmt)

                metadata = df[cols_metatable]
                metadata.columns = metadata.columns.str.replace(r'[()]', '')
                metadata.columns = metadata.columns.str.replace(r'[ /-]', '_', regex=True)
                metadata = metadata.replace('nan', np.nan)

                # metadata.to_sql('location_metadata',engine,schema='nobv_timeseries',index=None,if_exists='append')

                skeyz = sserieskey(fc, pkeygwm, locationkey, fskey[0],timestep='nonequidistant')
                flag = flagkeygwm

                dfx = pd.read_csv(os.path.join(root,file), delimiter=';', skiprows=nrrows, header = None, names = colnames)
                dfx.columns = timeseries
                dfx['datetime'] = pd.to_datetime(dfx['datetime'], format='%d-%m-%Y')
                dfx=dfx.dropna() 

                r=latest_entry(skeyz)

                if r!=dfx['datetime'].iloc[-1]:
                    dfx['timeserieskey'] = skeyz 
                    dfx['flags' ] = flag
                    dfx.to_sql('timeseriesvaluesandflags',engine,index=False,if_exists='append',schema='nobv_timeseries')
                else:
                    print('not updating')


            else:
                print('Different serieskey and / or flagkey needed')
# %%
