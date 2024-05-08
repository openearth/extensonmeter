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

# globals
geoserver_url = "https://service.pdok.nl/rws/ahn/wcs/v1_0"
layername = "dtm_05m"
cf = r"C:\develop\extensometer\connection_online.txt"
session, engine = establishconnection(cf)

if not testconnection(engine):
    print("Connecting to database failed")

# dictionary of tables to check for data in column altitude_msl
# key = tablename, value = columnname
# every table is indicating the table with locations and has as value the column
# that has measured surface elevation

dcttable = {}
dcttable["gwmonitoring.location"] = "altitude_msl"
dcttable["hhnktimeseries.location"] = "altitude_msl"
dcttable["nobv.location"] = "altitude_msl"
dcttable["hdsrtimeseries.location"] = "altitude_msl"
dcttable["hhnktimeseries.location"] = "altitude_msl"
dcttable["timeseries.location"] = "altitude_msl"


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


# Get locations from database
# convert xy to lon lat --> via query :)
for tbl in dcttable.keys():
    srid = getsrid(tbl)
    if srid != None:
        # create table location_mv, with ID and MV based on AHN
        nwtbl = tbl.replace("location", "location_metadata")
        strsql = f"create table if not exists {nwtbl} (locationkey integer primary key, mv double precision)"
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
            if x is not None:
                mv = getmv4point(x, y)
                print(lockey, x, y, mv)
            else:
                mv = "NULL"
                print(lockey, "from table", tbl, "has geometry is None")
            strsql = f"""insert into {nwtbl} (locationkey, mv) 
                        VALUES ({lockey},{mv})
                        ON CONFLICT(locationkey)
                        DO UPDATE SET
                        mv = {mv}"""
            engine.execute(strsql)


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
    strsql = f"""SELECT soilunit_code FROM soilmap.soilarea sa 
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
    preptable(nwtbl, "soilunit", "text")
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
        strsql = f"""insert into {nwtbl} (locationkey, soilunit) 
                     VALUES ({lockey},'{soildata}')
                        ON CONFLICT(locationkey)
                        DO UPDATE SET
                        soilunit = '{soildata}'"""
        engine.execute(strsql)


# following section calculates the width of a parcel based on the geometry
# requisite is a single polygon (in query below, a multipolygon is converted into a single
# polygon and the first polygon is selected, but.... .that is not necessarly the polygon where the point is in.... so
# first the polygon table needs to be converted into a single point table)
for tbl in dcttable.keys():
    nwtbl = tbl + "_metadata"
    preptable(nwtbl, "perceel_breedte_m", "double precision")
    strsql = f"""select (st_perimeter(ST_GeometryN(p.geom, 1)) 
	- sqrt((st_perimeter(ST_GeometryN(p.geom, 1))^2 - 16*ST_Area(ST_GeometryN(p.geom, 1)))/4 )) as width,
	st_perimeter(ST_GeometryN(p.geom, 1)) as perimiter, 
	ST_Area(ST_GeometryN(p.geom, 1)) as area, 
	locationkey from {tbl} t
    join input_parcels_2022 p on st_within(t.geom,p.geom)"""
    reswidth = engine.execute(strsql).fetchall()
    for i in range(len(reswidth)):
        width = reswidth[i][0]
        perim = reswidth[i][1]
        area = reswidth[i][2]
        lockey = reswidth[i][3]
        strsql = f"""insert into {nwtbl} (locationkey, soilunit) 
                     VALUES ({lockey},'{width}')
                        ON CONFLICT(locationkey)
                        DO UPDATE SET
                        perceel_breedte_m = '{width}'"""
        engine.execute(strsql)

# now we have all kinds of isolated tables with data neatly organised in the tables
# for reasons of overview, the following section combines all tables into 1 single view.
strsql = ''
for tbl in dcttable.keys():
    nwtbl = tbl+'_metadata'
    ansql = f"""SELECT geom, l.locationkey, altitude_msl as msrd_surface, mv as srfc_ahn4, soilunit, perceel_breedte_m FROM {tbl} l
            JOIN {nwtbl} mt on mt.locationkey = l.locationkey
            """
    strsql += ansql + ' UNION '

# remove the last uniton to get the following
drop view all_locations;
create or replace view all_locations as
SELECT geom, 'bro_'||l.locationkey::text, altitude_msl as msrd_surface, mv as srfc_ahn4, soilunit, perceel_breedte_m FROM gwmonitoring.location l
JOIN gwmonitoring.location_metadata mt on mt.locationkey = l.locationkey
UNION 
SELECT geom, 'hhnk_'||l.locationkey::text, altitude_msl as msrd_surface, mv as srfc_ahn4, soilunit, perceel_breedte_m FROM hhnktimeseries.location l
JOIN hhnktimeseries.location_metadata mt on mt.locationkey = l.locationkey
UNION 
SELECT geom, 'nobv_'||l.locationkey::text, altitude_msl as msrd_surface, mv as srfc_ahn4, soilunit, perceel_breedte_m FROM nobv.location l
JOIN nobv.location_metadata mt on mt.locationkey = l.locationkey
UNION 
SELECT geom, 'hdsr_'||l.locationkey::text, altitude_msl as msrd_surface, mv as srfc_ahn4, soilunit, perceel_breedte_m FROM hdsrtimeseries.location l
JOIN hdsrtimeseries.location_metadata mt on mt.locationkey = l.locationkey
UNION 
SELECT geom, 'wskip_'||l.locationkey::text, altitude_msl as msrd_surface, mv as srfc_ahn4, soilunit, perceel_breedte_m FROM timeseries.location l
JOIN timeseries.location_metadata mt on mt.locationkey = l.locationkey