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
# - assignment distance from well to various entities of top10

## various helper functions
from ts_helpders.ts_helpders import establishconnection, testconnection
from db_helpders import preptable


# setup connection
def setupconnecton(cf):
    session, engine = establishconnection(cf)
    return engine


# for every location the distance to ditch, road and centr of railroad is derived from top 10 data
# bear in mind, this is a very time costly operation, takes a long time (well, up to an hour)!
dcttop10 = {}
dcttop10["top10.top10nl_waterdeel_lijn"] = "distance_to_ditch_m"
dcttop10["top10.top10nl_spooras"] = "distance_to_railroad_m"
dcttop10["top10.top10nl_wegdeel_hartlijn"] = "distance_to_road_m"


def assign_t10(cf, tbl):
    """Update metadata table with the top10 by performing a spatial query on the soiltype database

    Args:
        cf  (string): link to connection file with credentials
        tbl (string): schema.table name with locations that act as basedata.

    Returns:
        ...
    """
    engine = setupconnecton(cf)
    for t10 in dcttop10.keys():
        c = dcttop10[t10]
        print("retrieving distances between points from ", tbl, " for ", t10)
        nwtbl = tbl + "_metadata"
        preptable(nwtbl, c, "double precision")
        strsql = f"""SELECT locationkey 
                FROM {tbl}"""
        locs = engine.execute(strsql).fetchall()
        for i in range(len(locs)):
            lockey = locs[i][0]
            strsql = f"""SELECT locationkey, 
                ST_DISTANCE(l.geom,wl.geom)
                FROM {tbl} l, {t10} wl
                WHERE locationkey = {lockey}
                ORDER BY
                l.geom <-> wl.geom
                limit 1"""
            vals = engine.execute(strsql).fetchall()

            strsql = f"""insert into {nwtbl} (well_id, {c}) 
                        VALUES ({lockey},{vals[0][1]})
                        ON CONFLICT(well_id)
                        DO UPDATE SET
                        {c} = {vals[0][1]}"""
            engine.execute(strsql)
    engine.dispose()
