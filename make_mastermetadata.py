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

# dictionary below is used to setup the mastertable that collects all data from the various schema's into 1 table

dctcolumns = {}
dctcolumns["well_id"] = "text"
dctcolumns["aan_id"] = "text"
dctcolumns["name"] = "text"
dctcolumns["transect"] = "text"
dctcolumns["parcel_type"] = "text"
dctcolumns["ditch_id"] = "text"
dctcolumns["ditch_name"] = "text"
dctcolumns["soil_class"] = "text"
dctcolumns["surface_level_m_nap"] = "double precision"
dctcolumns["start_date"] = "text"
dctcolumns["end_date"] = "text"
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
dctcolumns["tube_top"] = "double precision"
dctcolumns["tube_bot"] = "double precision"
dctcolumns["geometry"] = "text"  # WKT represenation of the geom of the location
dctcolumns["parcel_geom"] = "text"  # WKT represenation of the geom of the parcel
dctcolumns["selection"] = "text"

# globals
cf = r"C:\develop\extensometer\connection_online.txt"
session, engine = establishconnection(cf)

if not testconnection(engine):
    print("Connecting to database failed")

# dictionary of tables to check for data in column altitude_msl
# key = tablename, value = columnname
# every table is indicating the table with locations and has as value the column
# that has measured surface elevation


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


dcttable = {}
dcttable["bro_timeseries.location"] = "placeholder"
dcttable["hdsr_timeseries.location"] = "placeholder"
dcttable["hhnk_timeseries.location"] = "placeholder"
dcttable["wskip_timeseries.location"] = "placeholder"
dcttable["waterschappen_timeseries.location"] = "placeholder"  # handmetingen
dcttable["nobv_timeseries.location"] = "placeholder"  # nobv handmatige bewerkingen data


nwtbl = "metadata_ongecontroleerd.gwm"
strsql = f"""drop table if exists {nwtbl}; 
create table if not exists {nwtbl} (source serial primary key);"""
engine.execute(strsql)
print("table created", nwtbl)
for columname in dctcolumns.keys():
    print(columname)
    preptable(nwtbl, columname, dctcolumns[columname])

for tbl in dcttable.keys():
    n = tbl.split("_")[0]
    print("attempt to exectute queries for", n)
    if n == "nobv" or n == "waterschappen":
        strsql = f"""insert into {nwtbl} (well_id, name, aan_id, transect, parcel_type, ditch_id, ditch_name, soil_class, surface_level_m_nap, start_date, end_date, parcel_width_m, summer_stage_m_nap, winter_stage_m_nap, x_well, y_well, distance_to_ditch_m, trenches, trench_depth_m_sfl, wis_distance_m, wis_depth_m_sfl, tube_top, tube_bot, geometry, parcel_geom, selection)
        SELECT ('{n}_'||l.locationkey::text) as well_id, 
            l.name, 
            i.aan_id,
            '' as transect,
            'ref' as parcel_type,
            mt.ditch_id,
            '' as ditch_name, 
            i.archetype, 
            mt.surface_level_m_nap, 
            mt.start_date,
            mt.end_date,
            mt.parcel_width_m, 
            mt.summer_stage_m_nap,
            mt.winter_stage_m_nap, 
            mt.x_well,
            mt.y_well, 
            mt.distance_to_ditch_m,
            mt.trenches,
            mt.trench_depth_m_sfl,
            mt.wis_distance_m,
            mt.wis_depth_m_sfl,
            l.tubetop, 
            l.tubebot,
            st_astext(l.geom),
            st_astext(st_force2d(i.geom)),
            'yes' as selection
            FROM bro_timeseries.location l
            JOIN bro_timeseries.location_metadata mt on mt.well_id = l.locationkey
            JOIN bro_timeseries.timeseries t on t.locationkey = l.locationkey
            JOIN bro_timeseries.parameter p on p.parameterkey = t.parameterkey
            JOIN public.input_parcels_2022 i on st_within(l.geom,i.geom)
            where p.id = 'GWM'
            ON CONFLICT(source)
            DO NOTHING;"""
        engine.execute(strsql)

    else:
        strsql = f"""insert into {nwtbl} (well_id, name, aan_id, transect, parcel_type, ditch_id, ditch_name, soil_class, surface_level_m_nap, start_date, end_date, parcel_width_m, summer_stage_m_nap, winter_stage_m_nap, x_well, y_well, distance_to_ditch_m, trenches, trench_depth_m_sfl, wis_distance_m, wis_depth_m_sfl, tube_top, tube_bot, geometry, parcel_geom, selection)
        SELECT ('{n}_'||l.locationkey::text) as well_id,
        l.name, 
        i.aan_id,
        '' as transect,
        'ref' as parcel_type,
        mt.ditch_id,
        '' as ditch_name, 
        i.archetype, 
        mt.surface_level_m_nap, 
        mt.start_date,
        mt.end_date,
        mt.parcel_width_m, 
        mt.summer_stage_m_nap,
        mt.winter_stage_m_nap, 
        mt.x_well,
        mt.y_well, 
        mt.distance_to_ditch_m,
        mt.trenches,
        mt.trench_depth_m_sfl,
        mt.wis_distance_m,
        mt.wis_depth_m_sfl,
        l.tubetop, 
        l.tubebot,
        st_astext(l.geom),
        st_astext(st_force2d(i.geom)),
        'yes'
        FROM hhnk_timeseries.location l
        JOIN hhnk_timeseries.location_metadata mt on mt.well_id = l.locationkey
        JOIN public.input_parcels_2022 i on st_within(l.geom,i.geom)
        ON CONFLICT(source)
        DO NOTHING;"""
        engine.execute(strsql)

nwtbl = "metadata_ongecontroleerd.swm"
strsql = f"""drop table {nwtbl}; 
create table if not exists {nwtbl} (source text primary key)"""
engine.execute(strsql)
print("table created", nwtbl)
preptable(nwtbl, "name", "text")
preptable(nwtbl, "geom", "geometry(POINT, 28992)")

for tbl in dcttable.keys():
    n = tbl.split("_")[0]
    if n == "nobv" or n == "waterschappen":
        print(n)
        strsql = f"""insert into {nwtbl} (source, name, geom)
            SELECT ('{n}_'||l.locationkey::text) as source, l.name, l.geom FROM {n}_timeseries.location l
            JOIN {n}_timeseries.location_metadata mt on mt.well_id = l.locationkey
            JOIN {n}_timeseries.timeseries t on t.locationkey = l.locationkey
            JOIN {n}_timeseries.parameter p on p.parameterkey = t.parameterkey where p.id = 'SWM'
            ON CONFLICT(source)
            DO NOTHING;"""
        engine.execute(strsql)


strsql = f"""update metadata_ongecontroleerd.gwm gw
set veenperceel = TRUE
from public.input_parcels_2022 ip
WHERE ST_Contains(ip.geom, gw.geom);"""
engine.execute(strsql)

strsql = f"""drop table metadata_ongecontroleerd.all_locations; 
create table metadata_ongecontroleerd.all_locations as
select * from metadata_ongecontroleerd.gwm
where veenperceel = True and distance_to_railroad_m > 10 and distance_to_road_m > 10 and distance_to_ditch_m > 5;"""
engine.execute(strsql)

strsql = f"""WITH updated_values AS (
    SELECT DISTINCT ON (l.source) 
        l.source AS all_source, 
        swm.source AS swm_source,
        swm.name as ditch_name
    FROM
        public.peilvak_gw_sw p
    JOIN
        metadata_ongecontroleerd.all_locations l ON ST_DWithin(l.geom, p.geom, 0)
    LEFT JOIN
        metadata_ongecontroleerd.swm swm ON ST_DWithin(swm.geom, p.geom, 0)
    ORDER BY 
        l.source
)
UPDATE metadata_ongecontroleerd.all_locations
SET ditch_id = CASE 
                WHEN updated_values.swm_source IS NULL THEN NULL -- Keep ditch_id as NULL if swm_source is NULL
                ELSE updated_values.swm_source -- Update ditch_id with swm_source if it's not NULL
            END,
 ditch_name = CASE 
                WHEN updated_values.ditch_name IS NULL THEN NULL -- Keep ditch_name as NULL if swm_source is NULL
                ELSE updated_values.ditch_name -- Update ditch_name with swm_source if it's not NULL
            END
FROM updated_values
WHERE metadata_ongecontroleerd.all_locations.source = updated_values.all_source;"""
engine.execute(strsql)

strsql = f"""drop table metadata_ongecontroleerd.kalibratie; 
create table metadata_ongecontroleerd.kalibratie as
select * from metadata_ongecontroleerd.all_locations
where ditch_name is not Null;"""
engine.execute(strsql)

strsql = f"""drop table metadata_ongecontroleerd.validatie;
create table metadata_ongecontroleerd.validatie as
select * from metadata_ongecontroleerd.all_locations
where ditch_id is Null;"""
engine.execute(strsql)

print("created table kalibratie, validatie")
