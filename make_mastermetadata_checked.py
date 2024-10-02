# -*- coding: utf-8 -*-
# Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2024 Deltares
#   Gerrit Hendriksen (gerrit.hendriksen@deltares.nl)
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

# aim is to overwrite harvested data with expert judgement data for each record and each
# column

# import StringIO
import os
import pandas as pd
from sqlalchemy import text

## load several helper functions
from ts_helpers.ts_helpers import establishconnection, testconnection
from db_helpers import preptable, tablesetup, create_location_metadatatable

# globals
cf = r"C:\develop\extensometer\connection_online.txt"
# cf = r"C:\projecten\grondwater_monitoring\nobv\2023\connection_online_qsomers.txt"
session, engine = establishconnection(cf)

if not testconnection(engine):
    print("Connecting to database failed")


# dictionary of tables to check for data in column altitude_msl
# check if there is the manual edited data in schema
schema = "handmatige_aanpassingen"
strsql = """CREATE SCHEMA IF NOT EXISTS handmatige_aanpassingen"""
with engine.connect() as conn:
    res = conn.execute(text(strsql))


# because we need a clear defined table (dataformats), the first record is a dummy record with specified
# dataformats that will retrieved by the pd.read_excel function
def loadexpertjudgementdata(xlsx, ifexists):
    """Loads the provided expert data. The setup is expect to be exactly the same as
       the data in metadata_ongecontroleerd.kalibratie

    Args:
        cf  (string): link to connection file with credentials
        tbl (string): schema.table name with locations that act as basedata.
        metatable (string): schema.table name with metadata for each location

    Returns:
        ...
    """
    schema = "handmatige_aanpassingen"
    tbl = "kalibratie"
    df = pd.read_excel(xlsx, parse_dates=True)
    df.to_sql(
        tbl,
        engine,
        schema=schema,
        index=False,
        if_exists=ifexists,
    )

    # this first record will be removed with following query
    strsql = f"""delete from {schema}.{tbl} 
                where well_id='dummy'"""
    with engine.connect() as conn:
        conn.execute(text(strsql))
    return tbl


# todo make sure the last entered opbject is the final change.
"""select well_id, max(parcel_width_m),max(distance_to_ditch_m), max(changedate) from handmatige_aanpassingen.handmatige_aanpassingen_kalibratie
where well_id = 'nobv_3'
group by well_id"""


def checkval(tbl, well_id, column):
    """Function that checks retrieves the value from specified column and tbl
       using well_id as primary key

    Args: tbl     (string): reference to table (check schmema!)
          well_id (String): unique id of the location with possible data to be retrieved
          column  (String): columnname
    Return: val (*)       : any value in any data type, None by default if nothting manual has been filled
    """
    val = None
    try:
        strsql = f"""select {column} from {tbl} where well_id = '{well_id}'"""
        with engine.connect() as conn:
            res = conn.execute(text(strsql)).fetchall()
            val = res[0][0]
    except:
        val = None
    finally:
        return val


# step 1 load the data
# please set the if_exists option in a correct way,
# default is replace!, options are fail, replace or append
# !!!! IT IS EXPECTED TO HAVE A RECORD WHERE WELL_ID = dummy with for every column a value that
# reflects the datatype, because null is not a very good data type
xlsx = r"P:\11207812-somers-ontwikkeling\database_grondwater\handmatige_aanpassingen\handmatige_aanpassingen_kalibratie_v5-9-24.xlsx"
xlsx = r"P:\11207812-somers-ontwikkeling\database_grondwater\handmatige_aanpassingen\handmatige_aanpassingen_kalibratie_v20-9-24.xlsx"
tbl = loadexpertjudgementdata(xlsx, ifexists="replace")
tbl = "handmatige_aanpassingen.kalibratie"
# next version should have append.... so version control is implemented

# create new table in schema metadata_gecontroleerd
nwtbl = "metadata_gecontroleerd.kalibratie"
strsql = f"""drop table if exists {nwtbl}; """
with engine.connect() as conn:
    conn.execute(text(strsql))
    conn.commit()

# load the data that need to be checked/improved by handmatige data
untbl = "metadata_ongecontroleerd.kalibratie"

# make a copy from metadata_ongecontroleerd.kalibratie with selection = yes

# create_location_metadatatable(cf, nwtbl, dctcolumns)
strsql = f"create table {nwtbl} as select * from {untbl} where selection = 'yes'"
with engine.connect() as conn:
    conn.execute(text(strsql))
    conn.commit()
# in order to do updates a unique constraint needs to be set
strsql = f"ALTER TABLE {nwtbl} ADD CONSTRAINT unique_well UNIQUE (well_id);"
with engine.connect() as conn:
    conn.execute(text(strsql))
    conn.commit()


# following lines loop over de various records over the columns and check column wise if there
# is data in the manual table. The basis of the columns is defined in tablesetup in db_helpers!!
# important to realise is that if there is data in the expert judgement data that is not defined in a column
# in db_helpers (tablesetup), it will simply not be used in the checked table
dctcolumns = tablesetup()
dfo = pd.read_sql(f"select * from {untbl} where selection = 'yes'", engine)
lstcols = list(dfo)
for index, row in dfo.iterrows():
    well_id = row["well_id"]
    for c in lstcols:
        if c not in ("well_id", "geometry"):
            hmval = checkval(tbl, well_id, c)
            val = row[c]
            print(well_id, c, row[c], hmval)
            if hmval != None:
                val = hmval
            if str(val) == "nan" or str(val) == "[nan]":
                val = "null"
            if val is None:
                val = "null"
            if dctcolumns[c] == "text":
                strsql = f"""insert into {nwtbl} (well_id, {c}) 
                            VALUES ('{well_id}','{val}')
                            ON CONFLICT(well_id)
                            DO UPDATE SET
                            {c} = '{val}'"""
            elif dctcolumns[c] == "double precision[]":
                val = str(val).replace("[", "").replace("]", "").split(",")
                if len(val) == 1:
                    vls_dbl = "{" + str(val[0]) + "}"
                else:
                    vls_dbl = (
                        str(tuple([float(item) for item in val]))
                        .replace("(", "{")
                        .replace(")", "}")
                    )
                strsql = f"""insert into {nwtbl} (well_id, {c}) 
                            VALUES ('{well_id}','{vls_dbl}')
                            ON CONFLICT(well_id)
                            DO UPDATE SET
                            {c} = '{vls_dbl}'"""
            else:
                strsql = f"""insert into {nwtbl} (well_id, {c}) 
                            VALUES ('{well_id}',{val})
                            ON CONFLICT(well_id)
                            DO UPDATE SET
                            {c} = {val}"""
            with engine.connect() as conn:
                conn.execute(text(strsql))
                conn.commit()

user = "hendrik_gt"
strsql = f"reassign owned by {user} to qsomers"
with engine.connect() as conn:
    conn.execute(text(strsql))
    conn.commit()
