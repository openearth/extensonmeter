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
import matplotlib.pyplot as plt

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

def latest_entry(skey):
    """function to find the lastest timestep entry per skey. 
    input = skey
    output = pandas df containing either none or a date"""
    stmt="""select max(datetime) from waterschappen_timeseries.timeseriesvaluesandflags
        where timeserieskey={s};""".format(s=skey)
    r = engine.execute(stmt).fetchall()[0][0]
    r=pd.to_datetime(r) 
    return r 

def check_fc(skey):
    #TODO finish function
    """function to check if a certain waterschap is already in the location table """
    stmt="""select max(datetime) from waterschappen_timeseries.timeseriesvaluesandflags
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

def find_samenvattende_tabel(all_files):
    """
    Find the path of the 'SAMENVATTENDE_TABEL.txt' file in the list of files.
    
    Returns:
        str or None: Path of the 'SAMENVATTENDE_TABEL.txt' file if found, None otherwise.
    """
    for file_path in all_files:
        if os.path.basename(file_path) == "SAMENVATTENDE_TABEL.txt":
            return file_path

#TODO write function to check whether metadata has already been uploaded

# set reference to config file
local = True
if local:
    fc = r"C:\projecten\grondwater_monitoring\nobv\2023\connection_local_somers.txt"
else:
    fc = r"C:\develop\rwsdatatools\config_chloride_rpa.txt"
session,engine = establishconnection(fc)


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

#give root folder, if all files are in the same format, it should 
root = r'P:\11207812-somers-ontwikkeling\database_grondwater\handmatige_uitvraag_bestanden'

#location table
all_files = []
for root,subdirs,files in os.walk(root):    
    for file in files:
        if file.lower().endswith(".txt"):
            all_files.append(os.path.join(root, file))

# Find the path of "SAMENVATTENDE_TABEL.txt"

samenvattende_tabel_path = find_samenvattende_tabel(all_files)
if samenvattende_tabel_path: #if all metadatafiles recieve the same name
    #differenciate between the GWM metadata and the SWM metadata
    #part below is specifically for the SWM metadata
    #TODO write part for the GWM metadata
    #TODO rewrite, this part does not work in the way I anticipated
    df = pd.read_csv(samenvattende_tabel_path, delimiter=';')
    dfs = df.query('TYPE == "SWM"')
    dfg = df.query('TYPE == "GWM"')
    if dfs['TYPE'].unique()[0] == 'SWM':
        dfs = dfs.drop(columns = ['lat', 'lon', 'attributes', 'description'])
        dfs = dfs.rename(columns={'locationId': 'name', 
                                'TYPE': 'description',
                                'shortName': 'shortname'})
        dfs['epsgcode'] = 28992
        sf = samenvattende_tabel_path
        ws = str(os.path.join(root, file).split("\\")[-3]) #find the waterschap in the path
        fskey = loadfilesource(sf,fc, f"{ws}_metadata") #load filesourcekey
        dfs['locationkey']=dfs.index
        dfs['filesourcekey']=fskey[0][0] 

        # before uploading, we need to check if the metadata from this waterschap is already in the database
        # if it is, it can be skipped, if it is not, it needs to be uploaded. 
        # if new data is uploaded, the locationkey needs to be set accordingly

        #build in that the metadatafile is not updated when the entries are already there 

        # dfs.to_sql('location',engine,schema='waterschappen_timeseries',index=None,if_exists='append')

        # # update the table set the geometry for those records that have null as geom
        # stmt = """update {s}.{t} set geom = st_setsrid(st_point(x,y),epsgcode) where geom is null;""".format(s='waterschappen_timeseries',t='location')
        # engine.execute(stmt)

    elif dfg['TYPE'].unique()[0] == 'GWM':
        print('todo still')
    else:
        print('Data is not classified as GWM or SWM, Please find more information')
                
            
for file_path in all_files:
    # Skip "SAMENVATTENDE_TABEL.txt" as it has been processed separately
    if file_path == samenvattende_tabel_path:
        continue
    #use try-except clause so the program continues running but prints which names are not updated
    else:
        try:
            name=os.path.basename(file_path).split("_", 1)[1].rsplit('.',1)[0]
            data=os.path.basename(file_path).split("_", 1)[0] #find in name it is GWM or SWM
            nrrows, colnames, xycols, datum = skiprows(file_path)

            fskey = loadfilesource(file_path,fc,f"{name}_{data}")
            # header = pd.read_csv(os.path.join(root, file), sep=";", nrows=5) 
            # cols = [header.iloc[3,0] , header.iloc[4,0]]
            dfx = pd.read_csv(file_path, delimiter=';', skiprows=nrrows, header = None, names = colnames)

            #find the corresponding locationkey with the file name
            locationkey= list(dfs.loc[dfs['name'] == name, 'locationkey'])

            #to assign the correct serieskey and flagkey according to the type of data
            if data == 'SWM':
                skeyz = sserieskey(fc, pkeyswm, locationkey[0], fskey[0],timestep='nonequidistant')
                flag = flagkeyswm
            elif data == 'GWM':
                skeyz = sserieskey(fc, pkeygwm, locationkey[0], fskey[0],timestep='nonequidistant')
                flag = flagkeygwm
            else:
                print('Different serieskey and / or flagkey needed')

            #to check if the timeseries has already been updated in the database or not
            r=latest_entry(skeyz)
            #TODO check this
            dfx.columns.values[0] = "datetime"
            dfx.columns.values[1] = "scalarvalue"
            dfx['datetime'] = pd.to_datetime(dfx['datetime'], format='%d-%m-%Y %H:%M') 
            dfx=dfx.dropna()

            #only update if the timeseries has not been updated into the database so far
            if r!=dfx['datetime'].iloc[-1]:
                dfx['timeserieskey'] = skeyz 
                dfx['flags' ] = flag
                dfx.to_sql('timeseriesvaluesandflags',engine,index=False,if_exists='append',schema='waterschappen_timeseries')
            else:
                print('not updating')
        #plotting random timeseries from the list of files, not plotting every time (max 5 plots)
        except Exception as e:
            print(f"Not updating {name} due to: An error occurred: {e}")
            

            #plot een aantal random tijdseries en sla op

        
    
# %%
