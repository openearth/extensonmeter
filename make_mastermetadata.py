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
from ts_helpers.ts_helpers import establishconnection, testconnection

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
dctcolumns["ahn4_m_nap"] = "double precision"
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
dctcolumns["distance_wis"] = "double precision"
dctcolumns["z_well"] = "double precision"
dctcolumns["screen_top_m_sfl"] = "double precision"
dctcolumns["screen_bot_m_sfl"] = "double precision"
dctcolumns["altitude_msl"] = "double precision"
dctcolumns["geometry"] = (
    "geometry(POINT, 28992)"  # Point representation because it is used in further analysis
)
dctcolumns["parcel_geom"] = "text"  # WKT represenation of the geom of the parcel
dctcolumns["selection"] = "text"
dctcolumns["description"] = "text"

# globals
cf = r"C:\develop\extensometer\connection_online.txt"
# cf = r"C:\projecten\grondwater_monitoring\nobv\2023\connection_online_qsomers.txt"
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
    # NOBV and Waterschappen can have multipe parameters per location, only GWM now required.
    if n == "nobv" or n == "waterschappen":
        strsql = f"""insert into {nwtbl} (well_id, 
            aan_id,
            name,
            transect,
            parcel_type,
            ditch_id,
            ditch_name,
            soil_class,
            surface_level_m_nap,
            ahn4_m_nap,
            start_date,
            end_date,
            records,
            parcel_width_m,
            summer_stage_m_nap,
            winter_stage_m_nap,
            x_well,
            y_well,
            distance_to_ditch_m,
            trenches,
            trench_depth_m_sfl,
            wis_distance_m,
            wis_depth_m_sfl,
            distance_wis,
            z_well,
            screen_top_m_sfl,
            screen_bot_m_sfl,
            altitude_msl,
            geometry, 
            parcel_geom,
            selection,
            description)
        SELECT ('{n}_'||l.locationkey::text) as well_id, 
            i.aan_id,
            l.name, 
            mt.transect,
            mt.parcel_type,
            mt.ditch_id,
            '' as ditch_name, 
            i.archetype as soil_class,
            Null::double precision as surface_level_m_nap, 
            mt.surface_level_ahn4_m_nap as ahn4_m_nap, 
            mt.start_date,
            mt.end_date,
            mt.records,
            mt.parcel_width_m, 
            mt.summer_stage_m_nap,
            mt.winter_stage_m_nap, 
            st_x(l.geom),
            st_y(l.geom),            
            mt.distance_to_ditch_m,
            mt.trenches,
            mt.trench_depth_m_sfl,
            mt.wis_distance_m,
            mt.wis_depth_m_sfl,
            Null::double precision as distance_wis,
            l.z,
            l.tubetop as screen_top_m_sfl, 
            l.tubebot as screen_bot_m_sfl,
            l.altitude_msl,
            l.geom,
            st_astext(st_force2d(i.geom)),
            'yes' as selection,
            l.description
            FROM {n}_timeseries.location l
            JOIN {n}_timeseries.location_metadata2 mt on mt.well_id = l.locationkey
            JOIN {n}_timeseries.timeseries t on t.locationkey = l.locationkey
            JOIN {n}_timeseries.parameter p on p.parameterkey = t.parameterkey
            JOIN public.input_parcels_2022 i on st_within(l.geom,i.geom)
            where p.id = 'GWM' and mt.distance_to_railroad_m > 10 and mt.distance_to_road_m > 10 and mt.distance_to_ditch_m > 5
            ON CONFLICT(source)
            DO NOTHING;"""
        engine.execute(strsql)

    else:
        strsql = f"""insert into {nwtbl} (well_id, 
            aan_id,
            name,
            transect,
            parcel_type,
            ditch_id,
            ditch_name,
            soil_class,
            surface_level_m_nap,
            ahn4_m_nap,
            start_date,
            end_date,
            records,
            parcel_width_m,
            summer_stage_m_nap,
            winter_stage_m_nap,
            x_well,
            y_well,
            distance_to_ditch_m,
            trenches,
            trench_depth_m_sfl,
            wis_distance_m,
            wis_depth_m_sfl,
            distance_wis,
            z_well,
            screen_top_m_sfl,
            screen_bot_m_sfl,
            altitude_msl,
            geometry, 
            parcel_geom,
            selection,
            description)
        SELECT ('{n}_'||l.locationkey::text) as well_id, 
            i.aan_id,
            l.name, 
            mt.transect,
            mt.parcel_type,
            mt.ditch_id,
            '' as ditch_name, 
            i.archetype as soil_class,
            Null::double precision as surface_level_m_nap, 
            mt.surface_level_ahn4_m_nap as ahn4_m_nap, 
            mt.start_date,
            mt.end_date,
            mt.records,
            mt.parcel_width_m, 
            mt.summer_stage_m_nap,
            mt.winter_stage_m_nap, 
            st_x(l.geom),
            st_y(l.geom),            
            mt.distance_to_ditch_m,
            mt.trenches,
            mt.trench_depth_m_sfl,
            mt.wis_distance_m,
            mt.wis_depth_m_sfl,
            Null::double precision as distance_wis,
            l.z,
            l.tubetop as screen_top_m_sfl, 
            l.tubebot as screen_bot_m_sfl,
            l.altitude_msl,
            l.geom,
            st_astext(st_force2d(i.geom)),
            'yes' as selection,
            l.description
            FROM {n}_timeseries.location l
            JOIN {n}_timeseries.location_metadata2 mt on mt.well_id = l.locationkey
            JOIN {n}_timeseries.timeseries t on t.locationkey = l.locationkey
            JOIN {n}_timeseries.parameter p on p.parameterkey = t.parameterkey
            JOIN public.input_parcels_2022 i on st_within(l.geom,i.geom)
            where mt.distance_to_railroad_m > 10 and mt.distance_to_road_m > 10 and mt.distance_to_ditch_m > 5
            ON CONFLICT(source)
            DO NOTHING;"""
        engine.execute(strsql)

    strsql = f"""UPDATE {nwtbl} t
        SET parcel_width_m = i.sloot_afst
        FROM {n}_timeseries.location l
        JOIN {n}_timeseries.location_metadata2 mt ON mt.well_id = l.locationkey
        JOIN public.input_parcels_2022 i ON ST_Within(l.geom, i.geom)
        WHERE mt.distance_to_railroad_m > 10
        AND mt.distance_to_road_m > 10
        AND mt.distance_to_ditch_m > 5
        AND t.well_id = ('{n}_' || l.locationkey::text)
        AND t.parcel_width_m IS NULL;"""
    engine.execute(strsql)

nwtbl = "metadata_ongecontroleerd.swm"
strsql = f"""drop table if exists {nwtbl}; 
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
            JOIN {n}_timeseries.location_metadata2 mt on mt.well_id = l.locationkey
            JOIN {n}_timeseries.timeseries t on t.locationkey = l.locationkey
            JOIN {n}_timeseries.parameter p on p.parameterkey = t.parameterkey where p.id = 'SWM'
            ON CONFLICT(source)
            DO NOTHING;"""
        engine.execute(strsql)

# %%
# !!!! query does not work inside of python, but does work in pg admin. Run this part in PG admin
strsql = f"""WITH updated_values AS (
    SELECT DISTINCT ON (l.source) 
        l.well_id AS all_source, 
        swm.source AS swm_source,
        swm.name as ditch_name
    FROM
        public.peilvak_gw_sw p
    JOIN
        metadata_ongecontroleerd.gwm l ON ST_DWithin(l.geometry, p.geom, 0)
    LEFT JOIN
        metadata_ongecontroleerd.swm swm ON ST_DWithin(swm.geom, p.geom, 0)
    ORDER BY 
        l.source
)
UPDATE metadata_ongecontroleerd.gwm
SET ditch_id = CASE 
                WHEN updated_values.swm_source IS NULL THEN NULL 
                ELSE updated_values.swm_source 
            END,
 ditch_name = CASE 
                WHEN updated_values.ditch_name IS NULL THEN NULL 
                ELSE updated_values.ditch_name 
            END
FROM updated_values
WHERE metadata_ongecontroleerd.gwm.well_id = updated_values.all_source;"""
engine.execute(strsql)

# %%
strsql = f"""drop table if exists metadata_ongecontroleerd.kalibratie; 
create table metadata_ongecontroleerd.kalibratie as
select * from metadata_ongecontroleerd.gwm
where ditch_id is not Null;"""
engine.execute(strsql)

strsql = f"""drop table if exists metadata_ongecontroleerd.validatie;
create table metadata_ongecontroleerd.validatie as
select * from metadata_ongecontroleerd.gwm
where ditch_id is Null;"""
engine.execute(strsql)

print("created table kalibratie, validatie")

# bear in mind ownership of the tables
# does not work inside python and needs to be done in pgadmin
user = "hendrik_gt"
strsql = f"reassing owned by {user} to qsomers"
engine.execute(strsql)
