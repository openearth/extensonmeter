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

# base packages
import os
import pandas as pd

# import custum functions
from ts_helpders import establishconnection, testconnection

# globals
cf = r"C:\develop\extensometer\connection_online.txt"
session, engine = establishconnection(cf)
con = engine.connect()

if not testconnection(engine):
    print("Connecting to database failed")


dcttable = {}
dcttable["bro_timeseries.location"] = "placeholder"
dcttable["hdsr_timeseries.location"] = "placeholder"
dcttable["hhnk_timeseries.location"] = "placeholder"
dcttable["wskip_timeseries.location"] = "placeholder"
# dcttable["waterschappen_timeseries.location"] = "placeholder"  # handmetingen
dcttable["nobv_timeseries.location"] = "placeholder"  # nobv handmatige bewerkingen data


# todo ==> datetime as textformat

for tbl in dcttable.keys():
    n = tbl.split("_")[0]
    print("attempt to exectute queries for", n)

    strsql = f"""select well_id, min(datetime) as mindate,max(datetime) as maxdate from 
    {n}_timeseries.location l
    JOIN {n}_timeseries.location_metadata mt on mt.well_id = l.locationkey
    JOIN {n}_timeseries.timeseries t on t.locationkey = l.locationkey
    JOIN {n}_timeseries.parameter p on p.parameterkey = t.parameterkey
    JOIN {n}_timeseries.timeseriesvaluesandflags tsv on tsv.timeserieskey = t.timeserieskey
    group by well_id
    """
    res = pd.read_sql(strsql, con)
    for well_id, row in res.iterrows():
        print(well_id, row["mindate"], row["maxdate"])

    strsql = f"""insert into {nwtbl} (well_id, name, aan_id, transect, parcel_type, ditch_id, ditch_name, soil_class, surface_level_m_nap, start_date, end_date, parcel_width_m, summer_stage_m_nap, winter_stage_m_nap, x_well, y_well, distance_to_ditch_m, trenches, trench_depth_m_sfl, wis_distance_m, wis_depth_m_sfl, tube_top, tube_bot, geometry, parcel_geom, selection)

    ON CONFLICT(source)
    DO NOTHING;"""
    engine.execute(strsql)
