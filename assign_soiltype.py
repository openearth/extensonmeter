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
# - for every location querys (spatial query) the soilmap in the database and adds to soil_class

# import math
import time

# import StringIO
import os
from db_helpders import preptable


# Get locations from database
# convert xy to lon lat --> via query :)
# Using WMS (there doesn't seem to be a WFS deployed by PDOK) was a bit hard.
# therefore GPGK from https://www.pdok.nl/atom-downloadservices/-/article/bro-bodemkaart-sgm- has
# been downloaded and loaded into the database
def getdatafromdb(engine, x, y):
    """get point value soilunit

    Args:
        x (double pecision): longitude
        y (double pecision): latitude

    Returns:
        text: soilunit for given point
    """
    strsql = f"""SELECT su.soilunit_code FROM soilmap.soilarea sa 
    JOIN soilmap.soilarea_soilunit su on su.maparea_id = sa.maparea_id 
    WHERE st_within(st_geomfromewkt('SRID=28992;POINT({x} {y})'), sa.geom)"""
    try:
        scode = engine.execute(strsql).fetchone()[0]
        print("scode", scode)
        if scode == None:
            scode = "Null"
    except Exception:
        scode = "Null"
    return scode


def assign_soiltype(engine, tbl):
    """Update metadata table with the soiltype by performing a spatial query on the soiltype database

    Args:
        tbl (string): schema.table name with locations that act as basedata.

    Returns:
        ...
    """
    preptable(engine, tbl, "soil_class", "text")
    strsql = f"""select well_id, 
            x_well,
            y_well from {tbl}"""
    locs = engine.execute(strsql).fetchall()
    for i in range(len(locs)):
        lockey = locs[i][0]
        x = locs[i][1]
        y = locs[i][2]
        soildata = getdatafromdb(engine, x, y)
        strsql = f"""insert into {tbl} (well_id, soil_class) 
                     VALUES ({lockey},'{soildata}')
                        ON CONFLICT(well_id)
                        DO UPDATE SET
                        soil_class = '{soildata}'"""
        engine.execute(strsql)
