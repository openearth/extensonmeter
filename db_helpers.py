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
# dictionary below is used to setup the mastertable that collects all data from the various schema's into 1 table
def tablesetup():
    dctcolumns = {}
    dctcolumns["well_id"] = "text"
    dctcolumns["aan_id"] = "integer"
    dctcolumns["name"] = "text"
    dctcolumns["transect"] = "text"
    dctcolumns["parcel_type"] = "text"  # is er een maatregel ja/nee (standaard is ref)
    dctcolumns["ditch_id"] = "text"
    dctcolumns["ditch_name"] = "text"
    dctcolumns["soil_class"] = "text"
    dctcolumns["z_surface_level_m_nap"] = (
        "double precision"  # doorgegeven maaiveld door waterschap of dergelijke
    )
    dctcolumns["ahn4_m_nap"] = (
        "double precision"  # berekend maaiveld doormiddel van AHN
    )
    dctcolumns["start_date"] = "text"
    dctcolumns["end_date"] = "text"
    dctcolumns["records"] = "integer"
    dctcolumns["parcel_width_m"] = "double precision"
    dctcolumns["summer_stage_m_nap"] = "double precision"
    dctcolumns["winter_stage_m_nap"] = "double precision"
    dctcolumns["x_well"] = "double precision"
    dctcolumns["y_well"] = "double precision"
    dctcolumns["distance_to_ditch_m"] = "double precision"
    dctcolumns["trenches"] = "double precision[]"
    dctcolumns["trench_depth_m_sfl"] = "double precision"
    dctcolumns["wis_distance_m"] = "double precision"
    dctcolumns["wis_depth_m_sfl"] = "double precision"
    dctcolumns["distance_to_wis_m"] = "double precision"
    dctcolumns["screen_top_m_sfl"] = "double precision"
    dctcolumns["screen_bot_m_sfl"] = "double precision"
    dctcolumns["altitude_m_nap"] = "double precision"  # top buis
    dctcolumns["geometry"] = (
        "geometry(POINT, 28992)"  # Point representation because it is used in further analysis
    )
    dctcolumns["parcel_geom"] = "text"  # WKT represenation of the geom of the parcel
    dctcolumns["selection"] = "text"
    dctcolumns["description"] = "text"
    return dctcolumns


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


def create_location_metadatatable(cf, tbl, dctcolumns):
    """creates metadata table

    Args:
        engine (slqalchemy object): sqlalchemy engine object
        tbl (string): table incl. schema name
        dctcolumns (dictionary): dictionary where keys are fieldname and values are datatypes (PostgreSQL)
    """
    session, engine = establishconnection(cf)
    try:
        nwtbl = tbl
        strsql = f"create table if not exists {nwtbl} (well_id text primary key)"
        engine.execute(strsql)
        for columname in dctcolumns.keys():
            preptable(engine, nwtbl, columname, dctcolumns[columname])
    except Exception as e:
        print("following exception raised", e)
    finally:
        engine.dispose()
    return
