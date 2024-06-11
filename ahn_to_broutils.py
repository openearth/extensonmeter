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
# - derivation of AHN4 surface levels (DTM)
# - assignment of SOILTYPE from locally loaded SOILMAP
# - assignment of parcelwidth and distance of ditches?
# - assignment distance to roads or waterbodies


# import math
import time

# import StringIO
import os
import tempfile
import simplejson as json
import numpy as np
from pyproj import Proj, transform
from owslib.wms import WebMapService
from owslib.wcs import WebCoverageService
from osgeo import gdal
import rasterio

## Utils WCS [from fast]
from utils_wcs import *
from ts_helpders import establishconnection, testconnection

dctcolumns = {}
dctcolumns["well_id"] = "text"
dctcolumns["aan_id"] = "text"
dctcolumns["transect"] = "text"
dctcolumns["parcel_type"] = "text"
dctcolumns["x_centre_parcel"] = "double precision"
dctcolumns["y_centre_parcel"] = "double precision"
dctcolumns["soil_class"] = "text"
dctcolumns["surface_level_m_nap"] = "double precision"
dctcolumns["parcel_width_m"] = "double precision"
dctcolumns["summer_stage_m_nap"] = "double precision"
dctcolumns["winter_stage_m_nap"] = "double precision"
dctcolumns["ditch_id"] = "text"
dctcolumns["x_well"] = "double precision"
dctcolumns["y_well"] = "double precision"
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
dctcolumns["source"] = "text"


# TODO --> check parameter (process only groundwater wells for the master_metadata view) containing the TODO below 
# TODO --> join the SWM with corresponding GWM 
# TODO --> nadenken over het gebruik van source ipv well_id in de final master metadata

# globals
geoserver_url = "https://service.pdok.nl/rws/ahn/wcs/v1_0"
layername = "dtm_05m"
cf = r"C:\projecten\grondwater_monitoring\nobv\2023\connection_online_qsomers.txt"
session, engine = establishconnection(cf)

if not testconnection(engine):
    print("Connecting to database failed")

# dictionary of tables to check for data in column altitude_msl
# key = tablename, value = columnname
# every table is indicating the table with locations and has as value the column
# that has measured surface elevation

dcttable = {}
# dcttable["brotimeseries.location"] = "placeholder"
# dcttable["hdsrtimeseries.location"] = "placeholder"
# dcttable["hhnktimeseries.location"] = "placeholder"
# dcttable["timeseries.location"] = "placeholder"
dcttable["waterschappen_timeseries.location"] = "placeholder" #handmetingen
# dcttable["nobv_timeseries.location"] = "placeholder" #nobv handmatige bewerkingen data 


# Get a unique temporary file
def tempfile(tempdir, typen="plot", extension=".html"):
    fname = typen + str(time.time()).replace(".", "")
    return os.path.join(tempdir, fname + extension)


# Change XY coordinates general function
def change_coords(px, py, epsgin="epsg:3857", epsgout="epsg:4326"):
    outProj = Proj(init=epsgout)
    inProj = Proj(init=epsgin)
    return transform(inProj, outProj, px, py)


# Get Raster transect intersect [default 100m]
def getDatafromWCS(
    geoserver_url, layername, xst, yst, xend, yend, crs=28992, all_box=False
):
    linestr = "LINESTRING ({} {}, {} {})".format(xst, yst, xend, yend)
    print(crs, geoserver_url, layername)
    l = LS(linestr, crs, geoserver_url, layername)
    l.line()
    return l.intersect(all_box=all_box)  # coords+data


def cut_wcs(
    xst,
    yst,
    xend,
    yend,
    layername,
    owsurl,
    outfname,
    crs=4326,
    all_box=False,
    username=None,
    password=None,
) -> str:
    """
    Summary
        Implements the GetCoverage request with a given bbox from the user
    Args:
        xst (float): xmin
        yst (float): ymin
        xend (float): xmax
        yend (float): ymax
        layername (string): layername on geoserver
        owsurl (string): ows endpoint
        outfname (string): fname to store retrieved raster
        crs (int, optional): Defaults to 4326.
        all_box (bool, optional): Defaults to False.
    Returns:
        outfname (string)
    """
    linestr = "LINESTRING ({} {}, {} {})".format(xst, yst, xend, yend)
    ls = LS(linestr, crs, owsurl, layername, username, password)
    ls.line()
    ls.getraster(outfname, all_box=all_box)
    ls = None
    return outfname


def preptable(tbl, columname, datatype):
    """alters a table in the database and adds a column with a specified datatype if not existing

    Args:
        tbl (text): tablename
        columnname (text): columnname
        datatype (text): datatype (i.e. text, double precision, integer, boolean)

    Remark:
        In case of geometry column write out full datatype e.g. GEOMETRY POINT(28992)
    """
    strsql = f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS {columname} {datatype}"
    engine.execute(strsql)
    return


def getsrid(tbl):
    """Retrieve srid of specific table

    Args:
        tbl (_type_): schema+tablename to retrieve srid from

    Returns:
        _type_: EPSG code of geom column
    """
    schema = tbl.split(".")[0]
    table = tbl.split(".")[1]
    strsql = f"select find_srid('{schema}','{table}','geom')"
    srid = engine.execute(strsql).fetchone()[0]
    return srid


def getmv4point(x, y):
    """get point value for AHN4 from OGC WCS provided by PDOK

    Args:
        x (double pecision): longitude
        y (double pecision): latitude

    Returns:
        double precision: value of surface level for given point
    """
    xst = x - 0.0001
    xend = x + 0.0001
    yst = y - 0.0001
    yend = y + 0.0001

    arf = r"c:\temp\ding.tif"
    data = cut_wcs(xst, yst, xend, yend, layername, geoserver_url, arf, crs=4326)
    araster = rasterio.open(arf)
    row, col = araster.index(x, y)
    val = araster.read(1)[row, col]
    araster.close()
    os.unlink(arf)
    return val


# prepare master metadata table for alle subtables per source
for tbl in dcttable.keys():
    nwtbl = tbl + "_metadata"
    strsql = f"create table if not exists {nwtbl} (well_id integer primary key)"
    engine.execute(strsql)
    for columname in dctcolumns.keys():
        preptable(nwtbl, columname, dctcolumns[columname])
        # strsql = f'alter table {nwtbl} drop column {columname}'
        # engine.execute(strsql)


# Get locations from database
# convert xy to lon lat --> via query :)
for tbl in dcttable.keys():
    srid = getsrid(tbl)
    if srid != None:
        # create table location_mv, with ID and MV based on AHN
        nwtbl = tbl.replace("location", "location_metadata")
        strsql = f"create table if not exists {nwtbl} (well_id integer primary key, surface_level_m_nap double precision)"
        engine.execute(strsql)
        print("table created", nwtbl)
        # rquest locationky, xy in long lat for every record
        strsql = f"""select locationkey, 
        st_x(st_transform(geom,4326)),
        st_y(st_transform(geom,4326)) from {tbl}"""
        locwgs84 = engine.execute(strsql).fetchall()

        for i in range(len(locwgs84)):
            lockey = locwgs84[i][0]
            x = locwgs84[i][1]
            y = locwgs84[i][2]
            try:
                if x is not None:
                    mv = getmv4point(x, y)
                    print(lockey, x, y, mv)
                else:
                    mv = "NULL"
                    print(lockey, "from table", tbl, "has geometry None")
                strsql = f"""insert into {nwtbl} (well_id, surface_level_m_nap) 
                            VALUES ({lockey},{mv})
                            ON CONFLICT(well_id)
                            DO UPDATE SET
                            surface_level_m_nap = {mv}""" #add AHN values as surface_level_m_nap
                engine.execute(strsql)
            except:
                print('not updating AHN')

# Using WMS (there doesn't seem to be a WFS deployed by PDOK) was a bit hard.
# therefore GPGK from https://www.pdok.nl/atom-downloadservices/-/article/bro-bodemkaart-sgm- has
# been downloaded and loaded into the database
def getdatafromdb(x, y):
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


for tbl in dcttable.keys():
    nwtbl = tbl + "_metadata"
    preptable(nwtbl, "soil_class", "text")
    srid = getsrid(tbl)
    strsql = f"""select locationkey, 
            st_x(geom),
            st_y(geom) from {tbl}"""
    locs = engine.execute(strsql).fetchall()
    for i in range(len(locs)):
        lockey = locs[i][0]
        x = locs[i][1]
        y = locs[i][2]
        soildata = getdatafromdb(x, y)
        strsql = f"""insert into {nwtbl} (well_id, soil_class) 
                     VALUES ({lockey},'{soildata}')
                        ON CONFLICT(well_id)
                        DO UPDATE SET
                        soil_class = '{soildata}'"""
        engine.execute(strsql)


# set various generic (location dependend) data in metadata table (xy from well)
# also harvests centroid of the BRP_GEWAS parcel
for tbl in dcttable.keys():
    nwtbl = tbl + "_metadata"
    strsql = f"""SELECT locationkey, 
            st_x(l.geom),
            st_y(l.geom),
            st_x(st_centroid(brp.geom)) as x_centre_parcel,
	        st_y(st_centroid(brp.geom)) as y_centre_parcel from
            {tbl} l
            JOIN brp_gewas brp on st_within(l.geom,brp.geom)"""
    locs = engine.execute(strsql).fetchall()
    for i in range(len(locs)):
        lockey = locs[i][0]
        x = locs[i][1]
        y = locs[i][2]
        xc = locs[i][3]
        yc = locs[i][4]
        strsql = f"""insert into {nwtbl} (well_id, x_well,y_well,x_centre_parcel,y_centre_parcel) 
                    VALUES ({lockey},{x},{y},{xc},{yc})
                    ON CONFLICT(well_id)
                    DO UPDATE SET
                    x_well = {x}, y_well = {y}, x_centre_parcel = {xc}, y_centre_parcel = {yc}"""
        engine.execute(strsql)

# for every location the distance to ditch, road and centr of railroad is derived from top 10 data
# bear in mind, this is a very time costly operation, takes a long time (well, up to an hour)!
dcttop10 = {}
dcttop10["top10.top10nl_waterdeel_lijn"] = "distance_to_ditch_m"
dcttop10["top10.top10nl_spooras"] = "distance_to_railroad_m"
dcttop10["top10.top10nl_wegdeel_hartlijn"] = "distance_to_road_m"

for t10 in dcttop10.keys():
    c = dcttop10[t10]
    for tbl in dcttable.keys():
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

# ----- set various generic (location dependend) data in metadata table (xy from well)
# TODO check if this code is still needed? gives errors in its current state 
# and does not seme to add to the laction_metadata tables, may be left over code??
# Turn the code off for now

for tbl in dcttable.keys():
    nwtbl = tbl + "_metadata"
    strsql = f"""select locationkey, 
    perceel_id, 
    aan_id, 
    type_peilb, 
    zomerpeil_, 
    winterpeil, 
    sloot_afst, 
    x_coord, 
    y_coord from {tbl} l
    join input_parcels_2022 ip on st_within(l.geom, ip.geom) """
    locs = engine.execute(strsql).fetchall()
    for i in range(len(locs)):
        lockey = locs[i][0]
        x = locs[i][-2]
        y = locs[i][-1]
        try:
            strsql = f"""insert into {nwtbl} (well_id, x_well,y_well) 
                        VALUES ({lockey},{x},{y})
                        ON CONFLICT(well_id)
                        DO UPDATE SET
                        x_well = {x}, y_well = {y}"""
            engine.execute(strsql)
        except Exception as e:
            # Handle the conflict (e.g., log the error or ignore it)
            print(f"Error: {e}. {lockey}.")


# following section calculates the width of a parcel based on the geometry
# requisite is a single polygon (in query below, a multipolygon is converted into a single
# polygon and the first polygon is selected, but.... .that is not necessarly the polygon where the point is in.... so
# first the polygon table needs to be converted into a single point table)
# TODO check if this is being used
# for tbl in dcttable.keys():
#     nwtbl = tbl + "_metadata"
#     preptable(nwtbl, "parcel_width_m", "double precision")
#     strsql = f"""select (st_perimeter(ST_GeometryN(p.geom, 1)) 
# 	- sqrt((st_perimeter(ST_GeometryN(p.geom, 1))^2 - 16*ST_Area(ST_GeometryN(p.geom, 1)))/4 )) as width,
# 	st_perimeter(ST_GeometryN(p.geom, 1)) as perimiter, 
# 	ST_Area(ST_GeometryN(p.geom, 1)) as area, 
# 	locationkey from {tbl} t
#     join input_parcels_2022 p on st_within(t.geom,p.geom)"""
#     reswidth = engine.execute(strsql).fetchall()
#     for i in range(len(reswidth)):
#         width = reswidth[i][0]
#         perim = reswidth[i][1]
#         area = reswidth[i][2]
#         lockey = reswidth[i][3]
#         strsql = f"""insert into {nwtbl} (well_id, parcel_width_m) 
#                      VALUES ({lockey},'{width}')
#                         ON CONFLICT(well_id)
#                         DO UPDATE SET
#                         parcel_width_m = '{width}'"""
#         engine.execute(strsql)


# -------------- last stage of the proces is to union all metadata tables
# now we have all kinds of isolated tables with data neatly organised in the tables
# for reasons of overview, the following section combines all tables into 1 single view.
# TODO change -> see if it is possible to combine the union into a loop 
# strsql = ""
# for tbl in dcttable.keys():
#     nwtbl = tbl + "_metadata"
#     ansql = f"""SELECT geom, 
#                        l.locationkey, 
#                        altitude_msl as msrd_surface, 
#                        mv as srfc_ahn4, 
#                        soilunit, 
#                        perceel_breedte_m 
#             FROM {tbl} l
#             JOIN {nwtbl} mt on mt.locationkey = l.locationkey
#             """
#     strsql += ansql + " UNION "

# remove the last union to get the following sql, this should be adjusted to the new datamodel
"""drop table all_locations;
create table all_locations as
SELECT geom, ('bro_'||l.locationkey::text) as source, mt.parcel_width_m, mt.summer_stage_m_nap, mt.winter_stage_m_nap, mt.trenches, 
mt.trench_depth_m_sfl, mt.x_centre_parcel, mt.y_centre_parcel, mt.surface_level_m_nap, mt.ditch_id, mt.distance_to_ditch_m,
mt.distance_to_road_m, mt.distance_to_railroad_m, mt.distance_to_wis_m, mt.soil_class FROM brotimeseries.location l
JOIN brotimeseries.location_metadata mt on mt.well_id = l.locationkey
UNION 
SELECT geom, ('hhnk_'||l.locationkey::text) as source, mt.parcel_width_m, mt.summer_stage_m_nap, mt.winter_stage_m_nap, mt.trenches, 
mt.trench_depth_m_sfl, mt.x_centre_parcel, mt.y_centre_parcel, mt.surface_level_m_nap, mt.ditch_id, mt.distance_to_ditch_m,
mt.distance_to_road_m, mt.distance_to_railroad_m, mt.distance_to_wis_m, mt.soil_class FROM hhnktimeseries.location l
JOIN hhnktimeseries.location_metadata mt on mt.well_id = l.locationkey
UNION 
SELECT geom, ('nobv_'||l.locationkey::text) as source, mt.parcel_width_m, mt.summer_stage_m_nap, mt.winter_stage_m_nap, mt.trenches, 
mt.trench_depth_m_sfl, mt.x_centre_parcel, mt.y_centre_parcel, mt.surface_level_m_nap, mt.ditch_id, mt.distance_to_ditch_m,
mt.distance_to_road_m, mt.distance_to_railroad_m, mt.distance_to_wis_m, mt.soil_class FROM nobv_timeseries.location l
JOIN nobv_timeseries.location_metadata mt on mt.well_id = l.locationkey
JOIN nobv_timeseries.timeseries t on t.locationkey = l.locationkey
JOIN nobv_timeseries.parameter p on p.parameterkey = t.parameterkey
where p.id = 'GWM'
UNION 
SELECT geom, ('hdsr_'||l.locationkey::text) as source, mt.parcel_width_m, mt.summer_stage_m_nap, mt.winter_stage_m_nap, mt.trenches, 
mt.trench_depth_m_sfl, mt.x_centre_parcel, mt.y_centre_parcel, mt.surface_level_m_nap, mt.ditch_id, mt.distance_to_ditch_m,
mt.distance_to_road_m, mt.distance_to_railroad_m, mt.distance_to_wis_m, mt.soil_class FROM hdsrtimeseries.location l
JOIN hdsrtimeseries.location_metadata mt on mt.well_id = l.locationkey
UNION 
SELECT geom, ('wskip_'||l.locationkey::text) as source, mt.parcel_width_m, mt.summer_stage_m_nap, mt.winter_stage_m_nap, mt.trenches, 
mt.trench_depth_m_sfl, mt.x_centre_parcel, mt.y_centre_parcel, mt.surface_level_m_nap, mt.ditch_id, mt.distance_to_ditch_m,
mt.distance_to_road_m, mt.distance_to_railroad_m, mt.distance_to_wis_m, mt.soil_class FROM timeseries.location l
JOIN timeseries.location_metadata mt on mt.well_id = l.locationkey
UNION
SELECT geom, ('waterschappen_'||l.locationkey::text) as source, mt.parcel_width_m, mt.summer_stage_m_nap, mt.winter_stage_m_nap, mt.trenches, 
mt.trench_depth_m_sfl, mt.x_centre_parcel, mt.y_centre_parcel, mt.surface_level_m_nap, mt.ditch_id, mt.distance_to_ditch_m,
mt.distance_to_road_m, mt.distance_to_railroad_m, mt.distance_to_wis_m, mt.soil_class FROM waterschappen_timeseries.location l
JOIN waterschappen_timeseries.location_metadata mt on mt.well_id = l.locationkey
JOIN waterschappen_timeseries.timeseries t on t.locationkey = l.locationkey
JOIN waterschappen_timeseries.parameter p on p.parameterkey = t.parameterkey
where p.id = 'GWM'"""

"""
Alter table public.all_locations
add veenperceel boolean; 

update public.all_locations gw
set veenperceel = TRUE
from public.input_parcels_2022 ip
WHERE ST_Contains(ip.geom, gw.geom)

select * from public.all_locations
where veenperceel = True and distance_to_railroad_m > 10 and distance_to_road > 10 and distance_to_ditch > 5 

drop table public.all_swm;
create table public.all_swm as
SELECT geom, ('nobv_'||l.locationkey::text) as source, l.name FROM nobv.location l
JOIN nobv_timeseries.location_metadata mt on mt.well_id = l.locationkey
JOIN nobv_timeseries.timeseries t on t.locationkey = l.locationkey
JOIN nobv_timeseries.parameter p on p.parameterkey = t.parameterkey
where p.id = 'SWM'
UNION 
SELECT geom, ('waterschappen_'||l.locationkey::text) as source, l.name FROM waterschappen_timeseries.location l
JOIN waterschappen_timeseries.location_metadata mt on mt.well_id = l.locationkey --change into well_id
JOIN waterschappen_timeseries.timeseries t on t.locationkey = l.locationkey
JOIN waterschappen_timeseries.parameter p on p.parameterkey = t.parameterkey
where p.id = 'SWM'

#test query!
WITH updated_values AS (
    SELECT DISTINCT ON (x.source) 
        x.source AS x_source, 
        y.source AS y_source
    FROM
        public.peilvak_gw_sw p
    JOIN
        all_locations x ON ST_DWithin(x.geom, p.geom, 0)
    LEFT JOIN
        all_swm y ON ST_DWithin(y.geom, p.geom, 0)
    ORDER BY 
        x.source
)
UPDATE all_locations
SET ditch_id = CASE 
                WHEN updated_values.y_source IS NULL THEN NULL -- Keep ditch_id as NULL if y_source is NULL
                ELSE updated_values.y_source -- Update ditch_id with y_source if it's not NULL
            END
FROM updated_values
WHERE all_locations.source = updated_values.x_source;
"""
