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

# set of functions that gets data for every location available regarding a list of parameters:
# - derivation of AHN4 surface levels (DTM)
# - assignment of SOILTYPE from locally loaded SOILMAP
# - assignment of parcelwidth and distance of ditches?
# - assignment distance to roads or waterbodies

# import math
import time

# import StringIO
import os
import pandas as pd
from sqlalchemy import text

## Utils WCS [from fast]
from ts_helpers.ts_helpers import establishconnection, testconnection

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
    res = conn.execute(strsql)

# because we need a clear defined table (dataformats), the first record is a dummy record with specified
# dataformats that will retrieved by the pd.read_excel function
xlsx = r"C:\projectinfo\nl\NOBV\data\SOMERS_DATA\handmatige_aanpassingen_kalibratie_v30-7-24_V2.xlsx"
df = pd.read_excel(xlsx, parse_dates=True)
df.to_sql(
    "handmatige_aanpassingen_kalibratie",
    engine,
    schema=schema,
    index=False,
    if_exists="append",
)

# this first record will be removed with following query
strsql = text(
    """delete from handmatige_aanpassingen.handmatige_aanpassingen_kalibratie 
              where well_id=:v"""
)
strsql = strsql.bindparams(v="dummy")
with engine.connect() as conn:
    res = conn.execute(strsql)

# todo make sure the last entered opbject is the final change.
"""select well_id, max(parcel_width_m),max(distance_to_ditch_m), max(changedate) from handmatige_aanpassingen.handmatige_aanpassingen_kalibratie
where well_id = 'nobv_3'
group by well_id"""
