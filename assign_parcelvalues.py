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

# set of functions that gets data for every location available regarding a list of parameters:
# - for every location querys (spatial query) the input parcels

# import math
import time

# import StringIO
import os

from ts_helpers.ts_helpers import establishconnection, testconnection
from db_helpers import preptable

# ----- set various generic (location dependend) data in metadata table (xy from well)
# TODO check if this code is still needed? gives errors in its current state
# and does not seme to add to the laction_metadata tables, may be left over code??
# Turn the code off for now


def assign_parcelvalues(engine, tbl):
    """Update metadata table with the input_parcels by performing a spatial query

    Args:
        cf  (string): link to connection file with credentials
        tbl (string): schema.table name with locations that act as basedata.

    Returns:
        ...
    """
    loctable = ".".join([tbl.split(".")[0], tbl.split(".")[1].split("_")[0]])

    strsql = f"""select l.well_id, 
        perceel_id, 
        ip.aan_id, 
        type_peilb, 
        ROUND(zomerpeil_::numeric,2), 
        ROUND(winterpeil::numeric,2), 
        ROUND(sloot_afst::numeric,2), 
        ROUND(x_coord::numeric,2), 
        ROUND(y_coord::numeric,2) 
        from {tbl} l 
        join {loctable} loc on loc.locationkey = l.well_id
        join input_parcels_2022 ip on st_within(loc.geom, ip.geom)"""
    locs = engine.execute(strsql).fetchall()
    for i in range(len(locs)):
        lockey = locs[i][0]
        aan_id = locs[i][1]
        type_peilb = locs[i][2]
        zomerpeil_ = locs[i][3]
        winterpeil = locs[i][4]
        sloot_afst = locs[i][5]
        x_coord = locs[i][6]
        y_coord = locs[i][7]
        try:
            strsql = f"""insert into {tbl} (well_id, 
                            aan_id, 
                            parcel_type,
                            x_centre_parcel,
                            y_centre_parcel,
                            parcel_width_m,
                            summer_stage_m_nap,
                            winter_stage_m_nap) 
                        VALUES ({lockey},{x},{y})
                        ON CONFLICT(well_id)
                        DO UPDATE SET
                            aan_id = {aan_id}, 
                            parcel_type = {type_peilb},
                            x_centre_parcel = {x_coord},
                            y_centre_parcel = {y_coord},
                            parcel_width_m = {sloot_afst},
                            summer_stage_m_nap = {zomerpeil_},
                            winter_stage_m_nap = {winterpeil}"""
            engine.execute(strsql)
        except Exception as e:
            # Handle the conflict (e.g., log the error or ignore it)
            print(f"Error: {e}. {lockey}.")
    engine.dispose()
