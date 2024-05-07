# -*- coding: utf-8 -*-
# Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2016 Deltares
#       Joan Sala
#
#       joan.salacalero@deltares.nl
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

# $Id: emisk_utils.py 14127 2018-01-30 07:21:10Z hendrik_gt $
# $Date: 2018-01-30 08:21:10 +0100 (Tue, 30 Jan 2018) $
# $Author: hendrik_gt $
# $Revision: 14127 $
# $HeadURL: https://svn.oss.deltares.nl/repos/openearthtools/trunk/python/applications/wps/emisk/emisk_utils.py $
# $Keywords: $

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
from ts_helpders import establishconnection

# globals
geoserver_url = "https://service.pdok.nl/rws/ahn/wcs/v1_0"
layername = "dtm_05m"
cf = r"C:\develop\extensometer\connection_online.txt"
session, engine = establishconnection(cf)

def testconnection(engine):
    """Tests a connection and if it fails returns False

    Args:
        engine (sqlalchemy object): engine configuration

    Returns:
        Boolean: True if a test is succesfull
    """
    strsql = 'SELECT 1'
    a = True
    try:
        engine.execute(strsql)
    except Exception:
        print('database not working')
        a=False
    finally:
        return a
    

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


# dictionary of tables to check for data in column altitude_msl
# key = tablename, value = columnname

dcttable = {}
dcttable["hhnktimeseries.location"] = "altitude_msl"
dcttable["nobv.location"] = "altitude_msl"
dcttable["gwmonitoring.location"] = "altitude_msl"
dcttable["hdsrtimeseries.location"] = "altitude_msl"
dcttable["hhnktimeseries.location"] = "altitude_msl"
dcttable["timeseries.location"] = "altitude_msl"

# Get locations from database
# convert xy to lon lat --> via query :)
for tbl in dcttable.keys():
    srid = getsrid(tbl)
    if srid != None:
        # create table location_mv, with ID and MV based on AHN
        nwtbl = tbl.replace("location", "location_mv")
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


# following section uses WMS to find data from Soilmap of the NL, for given locations.
fclocal = r"C:\develop\extensometer\localhost_connection_somers.txt"
localsession, localengine = establishconnection(fclocal)


def getdatafromdb(x, y):
    """get point value soilunit

    Args:
        x (double pecision): longitude
        y (double pecision): latitude

    Returns:
        text: soilunit for given point
    """
    strsql = f"""SELECT soilunit_code FROM public.soilarea sa 
    JOIN public.soilarea_soilunit su on su.maparea_id = sa.maparea_id 
    WHERE st_within(st_geomfromewkt('SRID=28992;POINT({x} {y})'), sa.geom)"""
    scode = localengine.execute(strsql).fetchone()[0]
    print(scode)


def preptable(tbl, columnname, datatype):
    """alters a table in the database and adds a column with a specified datatype if not existing

    Args:
        tbl (text): tablename
        columnname (text): columnname
        datatype (text): datatype (i.e. text, double precision, integer, boolean)

    Remark:
        In case of geometry column write out full datatype e.g. GEOMETRY POINT(28992)
    """
    strsql = "alter table {tbl} add column if not exists {columname} {datatype}"
    engine.execute(strsql)
    return

for tbl in dcttable.keys():
    nwtbl = tbl + "_mv"
    preptable(nwtbl, "soilunit", "text")
    srid = getsrid(tbl)
    strsql = f"""select locationkey, 
            st_x(st_transform(geom,4326)),
            st_y(st_transform(geom,4326)) from {tbl}"""
    locwgs84 = engine.execute(strsql).fetchall()
    for i in range(len(locwgs84)):
        lockey = locwgs84[i][0]
        x = locwgs84[i][1]
        y = locwgs84[i][2]
        soildata = getdatafromdb(x, y)
        strsql = f"""insert into {nwtbl} (locationkey, soilunit) 
                     VALUES ({lockey},{soildata})
                        ON CONFLICT(locationkey)
                        DO UPDATE SET
                        soilunit = {soildata}"""
        engine.execute(strsql)


# following section calculates the width of a parcel based on the geometry
import geopandas
from shapely import geometry
from geoalchemy2.shape import to_shape 

def calculate_parcel_width(geometry):
    """
    The width is calculated using the formula for the width of a rectangular parcel.
    If the parcel is square-shaped, the width will default to NaN, and an alternative
    calculation method is called.
    """
    perimeter = geometry.length
    area = geometry.area
    width = (perimeter - np.sqrt(perimeter**2 - 16 * area)) / 4
    if np.isnan(width):  # if square
        width = np.sqrt(area)
    return width


def smooth_geometry(geometry):
    """
    This function applies a smoothing operation to the parcel geometry,
    removing small irregularities that could affect the calculation of its width.
    """
    buffer_distance = 10
    buffered_geometry = geometry.buffer(
        buffer_distance, 1, join_style=geometry.JOIN_STYLE.mitre
    )
    smoothed_geometry = buffered_geometry.buffer(
        -buffer_distance, 1, join_style=geometry.JOIN_STYLE.mitre
    )
    return smoothed_geometry


def get_slootafstand(gdf):
    """
    Calculate the width of a smoothed parcel add as column to existing GeoDataFrame.
    """
    gdf.loc[:, "smoothed"] = gdf["geometry"].apply(smooth_geometry)
    gdf.loc[:, "Sloot_afstand_m"] = gdf["smoothed"].apply(calculate_parcel_width)
    gdf = gdf.drop("smoothed", axis=1)
    return gdf

# for every point
# get peatparcel and put in a shapely geometry
x = 153313
y = 446205
for tbl in dcttable.keys():
    nwtbl = tbl + "_mv"
    preptable(nwtbl, "perceelbreedte", "double precision")
    preptable(nwtbl, "slootafstand", "double precision")
    srid = getsrid(tbl)
    tblpeat = 
    strsql = f"""select geom from {tblpeat} where st_within(st_geomfromewkt('SRID=28992;POINT({x} {y})'), geom)"""
