# -*- coding: utf-8 -*-
"""
Created on Thu Jan  7 14:49:58 2021

@author: hendrik_gt
"""

#  Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2021 Deltares for KINM (KennisImpuls Nutrienten Maatregelen)
#   Gerrit.Hendriksen@deltares.nl
#   Kevin Ouwerkerk
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

from ts_helpers.ts_helpers import establishconnection, testconnection

# setup of location_metadata table
dctcolumns = {}
dctcolumns["well_id"] = "text"
dctcolumns["aan_id"] = "text"
dctcolumns["name"] = "text"
dctcolumns["transect"] = "text"
dctcolumns["parcel_type"] = "text"
dctcolumns["x_centre_parcel"] = "double precision"
dctcolumns["y_centre_parcel"] = "double precision"
dctcolumns["soil_class"] = "text"
dctcolumns["ditch_id"] = "text"
dctcolumns["surface_level_ahn4_m_nap"] = "double precision"
dctcolumns["parcel_width_m"] = "double precision"
dctcolumns["summer_stage_m_nap"] = "double precision"
dctcolumns["winter_stage_m_nap"] = "double precision"
dctcolumns["x_well"] = "double precision"
dctcolumns["y_well"] = "double precision"
dctcolumns["z_surface_level_m_nap"] = "double precision"
dctcolumns["top_screen_m_mv"] = "double precision"
dctcolumns["bot_screen_m_mv"] = "double precision"
dctcolumns["distance_to_ditch_m"] = "double precision"
dctcolumns["distance_to_road_m"] = "double precision"
dctcolumns["distance_to_railroad_m"] = "double precision"
dctcolumns["distance_to_wis_m"] = "double precision"
dctcolumns["start_date"] = "text"
dctcolumns["end_date"] = "text"
dctcolumns["records"] = "integer"
dctcolumns["trenches"] = "double precision[]"
dctcolumns["trench_depth_m_sfl"] = "double precision"
dctcolumns["wis_distance_m"] = "double precision"
dctcolumns["wis_depth_m_sfl"] = "double precision"


def preptable(engine, tbl, columname, datatype):
    """alters a table in the database and adds a column with a specified datatype if not existing

    Args:
        engine : sqlalchemy engine object
        tbl (text): tablename
        columnname (text): columnname
        datatype (text): datatype (i.e. text, double precision, integer, boolean)

    Remark:
        In case of geometry column write out full datatype e.g. GEOMETRY POINT(28992)
    """
    try:
        strsql = f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS {columname} {datatype}"
        engine.execute(strsql)
    except Exception as e:
        print("following exception raised", e)
    finally:
        engine.dispose()
    return


def create_location_metadatatable(cf, tbl):
    """_summary_

    Args:
        engine (_type_): _description_
        tbl (_type_): _description_
    """
    session, engine = establishconnection(cf)
    try:
        nwtbl = tbl
        strsql = f"create table if not exists {nwtbl} (well_id integer primary key)"
        engine.execute(strsql)
        for columname in dctcolumns.keys():
            preptable(engine, nwtbl, columname, dctcolumns[columname])
    except Exception as e:
        print("following exception raised", e)
    finally:
        engine.dispose()
    return
