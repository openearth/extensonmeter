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
from sqlalchemy import exc, func, ARRAY, Float
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION

# local procedures
from orm_timeseries.orm_timeseries_waterschappen import (
    Base,
    FileSource,
    Location,
    Parameter,
    Unit,
    TimeSeries,
    TimeSeriesValuesAndFlags,
    Flags,
)
from ts_helpers.ts_helpers_waterschappen import (
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
    stmt = """select max(datetime) from waterschappen_timeseries.timeseriesvaluesandflags
        where timeserieskey={s};""".format(
        s=skey
    )
    r = engine.execute(stmt).fetchall()[0][0]
    r = pd.to_datetime(r)
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
        if ":" in line:  # Check if the line contains a colon
            key, value = line[1:].split(":")
            value = value.strip()  # Remove leading/trailing spaces
            data[key.strip()] = (
                value if value else np.nan
            )  # Replace empty values with NaN

    # Create a DataFrame
    df = pd.DataFrame([data])
    return df


def find_locationkey():
    # find the max locationkey which is currently stored in the database
    stmt = """select max(locationkey) from waterschappen_timeseries.location;"""
    r = engine.execute(stmt).fetchall()[0][0]
    return r


def find_if_stored(name):
    # find the max locationkey which is currently stored in the database
    try:
        stmt = """select locationkey from waterschappen_timeseries.location
        where name = '{n}';""".format(
            n=name
        )
        r = engine.execute(stmt).fetchall()[0][0]
        return r, True  # mean yes / True it is stored
    except:
        return False


# set reference to config file
local = True
if local:
    fc = r"C:\projecten\grondwater_monitoring\nobv\2023\connection_local_somers.txt"
else:
    fc = r"C:\projecten\grondwater_monitoring\nobv\2023\connection_online_qsomers.txt"
session, engine = establishconnection(fc)

# manual input, give path to the root folders to loop over
path_1 = r"P:\11207812-somers-ontwikkeling\database_grondwater\handmatige_uitvraag_bestanden\Rivierenland"
# path_2 = r'P:\11207812-somers-ontwikkeling\database_grondwater\handmatige_uitvraag_bestanden\Delfland\SOMERS_DATA' #heel veel data maar bijna geen locatie komt terug voor de analyse
path_3 = r"P:\11207812-somers-ontwikkeling\database_grondwater\handmatige_uitvraag_bestanden\HDSR\geschikte_data"
path_4 = r"P:\11207812-somers-ontwikkeling\database_grondwater\handmatige_uitvraag_bestanden\AGV"  # gebruikt spaties als sep in de GWM maar niet in de SWM
# path_5 = r'P:\11207812-somers-ontwikkeling\database_grondwater\handmatige_uitvraag_bestanden\HunzeenAas\bewerkt' #datum tijd notatie klopt nog niet
path_6 = r"P:\11207812-somers-ontwikkeling\database_grondwater\handmatige_uitvraag_bestanden\Wetterskip"  # (string replace has to be on to work with this data)

# Create a list of paths
# paths = [path_1, path_2]
paths = [path_6]

# assigning parameters, either grondwaterstand or slootwaterpeil
# zoetwaterstijghoogtes
pkeygwm = sparameter(
    fc, "GWM", "Grondwatermeetpunt", ["m-NAP", "meter NAP"], "Grondwatermeetpunt"
)
pkeyswm = sparameter(
    fc, "SWM", "Slootwatermeetpunt", ["m-NAP", "meter NAP"], "Slootwatermeetpunt"
)

tstkeye = stimestep(session, "nonequidistant", "")

flagkeygwm = sflag(fc, "Grondwatermeetpuntt-ruwe data", "Grondwatermeetpunt-ruwe data")
flagkeyswm = sflag(fc, "Slootwatermeetpunt-ruwe data", "Slootwatermeetpunt-ruwe data")

# %%
# define which columns will be selected
cols_loctable = [
    "naam_meetpunt",
    "x-coor",
    "y-coor",
    "top filter (m-mv)",
    "onderkant filter (m-mv)",
    "maaiveld (m NAP)",
]
cols_metatable = [
    "slootafstand (m)",
    "zomer streefpeil (m NAP)",
    "winter streefpeil (m NAP)",
    "greppelafstand (m)",
    "greppeldiepte (m-mv)",
    "WIS afstand (m)",
    "WIS diepte (m-mv)",
]

new_loctabel = ["name", "x", "y", "tubetop", "tubebot", "z"]
new_loc_swm = ["name", "x", "y"]
timeseries = ["datetime", "scalarvalue"]

# %%
# Loop over each path
for root in paths:
    for root, subdirs, files in os.walk(root):
        for count, file in enumerate(files):
            if file.lower().endswith(".txt"):
                name = os.path.basename(file).split("_", 1)[1].rsplit(".", 1)[0]
                wb_name = os.path.basename(root)
                name = name.replace("[", "").replace("]", "")
                name = wb_name + "_" + name
                data = os.path.basename(file).split("_", 1)[
                    0
                ]  # find in name it is GWM or SWM
                nrrows, colnames, xycols, datum = skiprows(os.path.join(root, file))
                y = find_if_stored(name)
                # print(y)

                if y == False:
                    x = find_locationkey()
                    if x is None:
                        locationkey = 0
                    else:
                        locationkey = x + 1
                    print("Location not stored yet:", name)

                else:
                    locationkey = y[0]
                    print("location already stored:", name)

                fskey = loadfilesource(os.path.join(root, file), fc, f"{name}_{data}")
                dfx = pd.read_csv(
                    os.path.join(root, file),
                    delimiter=";",
                    skiprows=nrrows,
                    header=None,
                    names=colnames,
                )
                # print(dfx)
                if data == "SWM":
                    df = extract_info_from_text_file(os.path.join(root, file))
                    df.columns = new_loc_swm

                    string_columns = df.select_dtypes(include=["object"]).columns
                    for col in string_columns:
                        if col != "name":
                            df[col] = df[col].apply(lambda x: x.replace(",", "."))
                            df[col] = df[col].astype(float)

                    df["locationkey"] = locationkey
                    df["epsgcode"] = 28992
                    df["filesourcekey"] = fskey[0][0]

                    if y == False:
                        df.to_sql(
                            "location",
                            engine,
                            schema="waterschappen_timeseries",
                            index=None,
                            if_exists="append",
                        )
                        stmt = """update {s}.{t} set geom = st_setsrid(st_point(x,y),epsgcode) where geom is null;""".format(
                            s="waterschappen_timeseries", t="location"
                        )
                        engine.execute(stmt)

                    skeyz = sserieskey(
                        fc, pkeyswm, locationkey, fskey[0], timestep="nonequidistant"
                    )
                    flag = flagkeyswm

                    dfx = pd.read_csv(
                        os.path.join(root, file),
                        delimiter=";",
                        skiprows=nrrows,
                        header=None,
                        names=colnames,
                    )
                    dfx.columns = timeseries
                    dfx["datetime"] = dfx["datetime"].str.replace(
                        "24:00:00", "00:00:00"
                    )  # if they do not use the correct datetime format
                    try:
                        dfx["datetime"] = pd.to_datetime(
                            dfx["datetime"], format="%d-%m-%Y"
                        )
                    except ValueError:
                        try:
                            dfx["datetime"] = pd.to_datetime(
                                dfx["datetime"], format="%d-%m-%Y %H:%M:%S"
                            )
                        except:
                            dfx["datetime"] = pd.to_datetime(
                                dfx["datetime"], format="%d-%m-%Y %H:%M"
                            )

                    # dfx['scalarvalue'] = dfx['scalarvalue'].str.replace(",", ".")
                    dfx["scalarvalue"] = dfx["scalarvalue"].astype("float64")
                    dfx = dfx.dropna()
                    dfx.sort_values(by=["datetime"], inplace=True)

                    r = latest_entry(
                        skeyz
                    )  # find if there was already a timeseries stored in the database
                    duplicate_rows = dfx[dfx.duplicated()]  # find duplicated entries
                    # print(duplicate_rows)
                    dfx = dfx.drop_duplicates()
                    print(r, skeyz)
                    if r != dfx["datetime"].iloc[-1]:
                        dfx["timeserieskey"] = skeyz
                        dfx["flags"] = flag
                        dfx.to_sql(
                            "timeseriesvaluesandflags",
                            engine,
                            index=False,
                            if_exists="append",
                            schema="waterschappen_timeseries",
                        )
                    else:
                        print("not updating")

                elif data == "GWM":
                    df = extract_info_from_text_file(os.path.join(root, file))
                    locationtable = df[cols_loctable]
                    locationtable.columns = new_loctabel

                    string_columns = locationtable.select_dtypes(
                        include=["object"]
                    ).columns  # when people send things with comma's instead of points as delimiters
                    for col in string_columns:  # convert str data
                        if col != "name":
                            locationtable[col] = locationtable[col].apply(
                                lambda x: x.replace(",", ".")
                            )
                            locationtable[col] = locationtable[col].astype(float)

                    # add epsg and add locationkey
                    locationtable["locationkey"] = locationkey
                    locationtable["epsgcode"] = 28992
                    locationtable["filesourcekey"] = fskey[0][0]

                    if y == False:
                        locationtable.to_sql(
                            "location",
                            engine,
                            schema="waterschappen_timeseries",
                            index=None,
                            if_exists="append",
                        )
                        stmt = """update {s}.{t} set geom = st_setsrid(st_point(x,y),epsgcode) where geom is null;""".format(
                            s="waterschappen_timeseries", t="location"
                        )
                        engine.execute(stmt)

                        metadata = df[cols_metatable]
                        metadata = metadata.rename(
                            columns={
                                "slootafstand (m)": "parcel_width_m",
                                "greppelafstand (m)": "trenches",
                                "greppeldiepte (m-mv)": "trench_depth_m_sfl",
                                "zomer streefpeil (m NAP)": "summer_stage_m_nap",
                                "winter streefpeil (m NAP)": "winter_stage_m_nap",
                                "WIS afstand (m)": "wis_distance_m",
                                "WIS diepte (m-mv)": "wis_depth_m_sfl",
                            }
                        )

                        string_columns = metadata.select_dtypes(
                            include=["object"]
                        ).columns
                        for col in string_columns:
                            if col != "name":
                                metadata[col] = metadata[col].apply(
                                    lambda x: x.replace(",", ".")
                                )
                                metadata[col] = metadata[col].astype(float)

                        metadata = metadata.replace("nan", np.nan)
                        metadata["trenches"] = metadata["trenches"].apply(lambda x: [x])
                        metadata["well_id"] = locationkey

                        dtype = {
                            "trenches": ARRAY(
                                DOUBLE_PRECISION
                            )  # making sure trenches is exported as a double precision array
                        }
                        # metadata.to_sql('location_metadata',engine,schema='waterschappen_timeseries',index=None,if_exists='append', dtype=dtype)

                    skeyz = sserieskey(
                        fc, pkeygwm, locationkey, fskey[0], timestep="nonequidistant"
                    )
                    flag = flagkeygwm

                    dfx = pd.read_csv(
                        os.path.join(root, file),
                        sep=";",
                        skiprows=nrrows,
                        header=None,
                        names=colnames,
                    )
                    dfx.columns = timeseries
                    dfx["datetime"] = dfx["datetime"].str.replace(
                        "24:00:00", "00:00:00"
                    )
                    try:
                        dfx["datetime"] = pd.to_datetime(
                            dfx["datetime"], format="%d-%m-%Y"
                        )
                    except ValueError:
                        try:
                            dfx["datetime"] = pd.to_datetime(
                                dfx["datetime"], format="%d-%m-%Y %H:%M:%S"
                            )
                        except:
                            dfx["datetime"] = pd.to_datetime(
                                dfx["datetime"], format="%d-%m-%Y %H:%M"
                            )
                    # dfx['scalarvalue'] = dfx['scalarvalue'].str.replace(",", ".")
                    dfx["scalarvalue"] = dfx["scalarvalue"].astype("float64")
                    dfx = dfx.dropna()

                    r = latest_entry(skeyz)
                    duplicate_rows = dfx[dfx.duplicated()]  # if data has duplicated
                    # print(duplicate_rows)
                    dfx = dfx.drop_duplicates()  # remove the duplicated
                    dfx.sort_values(by=["datetime"], inplace=True)

                    print(r, skeyz)

                    if r != dfx["datetime"].iloc[-1]:
                        dfx["timeserieskey"] = skeyz
                        dfx["flags"] = flag
                        dfx.to_sql(
                            "timeseriesvaluesandflags",
                            engine,
                            index=False,
                            if_exists="append",
                            schema="waterschappen_timeseries",
                        )
                    else:
                        print("not updating")

                else:
                    print("NOT SWM or GWM:", name)
#     # %%
# %% FIXING THE TUBEBOTS THAT WENT WRONG. STORE IN CODE JUST IN CASE
# #assign a new locationkey
# #first find if location is already stored in the database, if not stored, the following code will be run
# #if data == 'GWM':
#     y = find_if_stored(name)

#     nwtbl = 'waterschappen_timeseries.location'
#     df= extract_info_from_text_file(os.path.join(root,file))
#     locationtable = df[cols_loctable]
#     locationtable.columns = new_loctabel

#     string_columns = locationtable.select_dtypes(include=['object']).columns
#     for col in string_columns:
#         if col != 'name':
#             locationtable[col] = locationtable[col].apply(lambda x: x.replace(',', '.'))
#             locationtable[col] = locationtable[col].astype(float)


#     strsql = f"""insert into {nwtbl} (locationkey, tubebot)
#             VALUES ({y[0]},'{locationtable.tubebot.values[0]}')
#             ON CONFLICT(locationkey)
#             DO UPDATE SET
#             tubebot = '{locationtable.tubebot.values[0]}'"""
#     engine.execute(strsql)


# #%% updating the description in the location table
# df = pd.read_excel(r'P:\11207812-somers-ontwikkeling\database_grondwater\handmatige_uitvraag_bestanden\Wetterskip\240703 Export_meetpunten_VW_monitoring.xlsx', skiprows=1)
# cols = ['# naam_meetpunt','maatregel' ]
# df = df[cols]
# df = df.drop(120)

# nwtbl = 'waterschappen_timeseries.location'

# for index, row in df.iterrows():
#     lockey = find_if_stored(row[0])

#     if lockey[1] == True:
#         print(lockey)
#         strsql = f"""insert into {nwtbl} (locationkey, description)
#                 VALUES ({lockey[0]},'{row[1]}')
#                 ON CONFLICT(locationkey)
#                 DO UPDATE SET
#                 description = '{row[1]}'"""
#         engine.execute(strsql)
# %%
