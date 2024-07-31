# -*- coding: utf-8 -*-
# Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2024 Deltares
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

## some helper functions
from ts_helpers.ts_helpers import establishconnection, testconnection
from db_helpers import create_location_metadatatable
import assign_soiltype
import assign_parcelvalues
import assign_ahn4
import assign_top10
import assign_timeseriesstats

# globals
cf = r"C:\projecten\grondwater_monitoring\nobv\2023\connection_online_qsomers.txt"
cf = r"C:\develop\extensometer\connection_online.txt"

session, engine = establishconnection(cf)

# -------------- section for BRO data
# step 1. setup location_metadata table
# step 2. fill with location data for selection of data
# step 3. assign ahn4
# step 4. assign soiltype
# step 5. assign parcelvalues
# step 6. assign top10
# step 7. assign timeseriesstats (min, max and nr. records data)
# step 8. compile info to location.metadata table

# 1 setup metadata table (tbl should be new name)
tbl = "bro_timeseries.location"
nwtbl = "bro_timeseries.location_metadata2"
create_location_metadatatable(cf, nwtbl)

# 2 BRO specific
# this part is different for every source, since the data is not exactly the same
# for BRO data, all values are with respect to reference level (m-NAP), while model expects m-mv
strsql = """
SELECT 
	locationkey,
	st_x(geom),
	st_y(geom),
	z as z_surface_level_m_nap,
	ROUND((z-tubetop)::numeric,2) as top_screen_m_mv,
	ROUND((z-tubebot)::numeric,2) as bot_screen_m_mv
FROM bro_timeseries.location
order by locationkey
"""
locs = engine.execute(strsql).fetchall()
for i in range(len(locs)):
    lockey = locs[i][0]
    x = locs[i][1]
    y = locs[i][2]
    z = locs[i][3]
    zt = locs[i][4]
    zb = locs[i][5]
    try:
        strsql = f"""insert into {nwtbl} (well_id, x_well,y_well,z_surface_level_m_nap,top_screen_m_mv,bot_screen_m_mv) 
                    VALUES ({lockey},{x},{y}, {z}, {zt},{zb})
                    ON CONFLICT(well_id)
                    DO UPDATE SET
                    x_well = {x}, y_well = {y}, z_surface_level_m_nap = {z}, top_screen_m_mv = {zt}, bot_screen_m_mv = {zb}"""
        engine.execute(strsql)
    except Exception as e:
        # Handle the conflict (e.g., log the error or ignore it)
        print(f"Error: {e}. {lockey}.")

# 3 assign ahn4 (needs some small changes to get it working)
# need of geometry column for conversion to Lat-long, it is expected that geom is in 28992
assign_ahn4.assign_ahn(engine, tbl, nwtbl)

# 4 assign soiltype
assign_soiltype.assign_soiltype(engine, nwtbl)

# 5 assign parcelvalues
assign_parcelvalues.assign_parcelvalues(engine, nwtbl)

# 6 assign_top10
assign_top10.assign_t10(engine, tbl, nwtbl)

# 7 assign timeseries timewindow and number of records
assign_timeseriesstats.settimeseriesstats(engine, tbl, nwtbl)
