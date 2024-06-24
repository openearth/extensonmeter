# -*- coding: utf-8 -*-
"""
Created on Tue Jul 19 12:05:14 2022

@author: hendrik_gt

#  Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2023 Deltares for Projects with a FEWS datamodel in
#                 PostgreSQL/PostGIS database used various project
#   Gerrit Hendriksen@deltares.nl
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

"""

# TODO -- bear in mind that the target shema's are renamed!!!


# base link is https://gis.wetterskipfryslan.nl/arcgis/rest/services/Grondwatersite_mpn/MapServer/1/query?outFields=*&orderByFields=MONSTERDATUM&f=json&where=TELEMETRIELOCATIEID%3D%27L6206%27+AND+MONSTERDATUM+%3E+date+%272021-8-2+00%3A00%3A00%27+

import os
import requests
from datetime import datetime
import pandas as pd

from orm_timeseries import TimeSeriesValuesAndFlags as tsv
from ts_helpders import loadfilesource, location, establishconnection, Location
from ts_helpders import sparameter, stimestep, sflag, sserieskey

local = False
if local:
    fc = r"C:\develop\extensometer\localhost_connection.txt"
else:
    fc = r"C:\develop\extensometer\connection_online.txt"

mapserver_url = "https://gis.wetterskipfryslan.nl/arcgis/rest/services/Grondwatersite_mpn/MapServer/"
session, engine = establishconnection(fc)


def procesdata(jsonrespons, fid):
    """Consumes the jsonrespons and stores:
       location, flag, parameter, serieskey and loads the data by calling storetimeseries

    Args:
        jsonrespons (json): first layer with location, parameter information
        fid (integer): file identifier store in the data model
    """

    features = jsonrespons["features"]
    for i in range(len(features)):
        ogw = None
        dgw = None
        attrib = features[i]["attributes"]
        loc = attrib["LOCATIE"]
        nam = attrib["NAAM"]
        ogw = attrib["ONDIEPGRONDWATER"]
        dgw = attrib["DIEPGRONDWATER"]
        fm = attrib["FREQUENTIEMETING"]
        tid = attrib["TELEMETRIELOCATIEID"]
        des = attrib["OMSCHRIJVING"]
        mv = attrib["MAAIVELD"]
        x = jsonrespons["features"][i]["geometry"]["x"]
        y = jsonrespons["features"][i]["geometry"]["y"]
        print(tid, nam)
        # set location parameter
        lid = location(
            fc, fid, nam, x, y, epsg=4326, shortname=tid, description=des, z=mv
        )
        # set the flag key to validated
        flagid = sflag(fc, "validated")

        # set the parameterkey and the serieskey
        # some locations have 2 filters, for now this is only indicated by ogw, dgw
        if ogw == "j" and dgw == "j":
            pid = sparameter(
                fc,
                "ondiepgrondwater",
                "ondiepgrondwater",
                ("stand", "m-NAP"),
                "ondiepgrondwater",
            )
            sid = sserieskey(fc, pid, lid, fid, fm)
            pid2 = sparameter(
                fc,
                "diepgrondwater",
                "diepgrondwater",
                ("stand", "m-NAP"),
                "diepgrondwater",
            )
            sid2 = sserieskey(fc, pid2, lid, fid, fm)
            # store timeseries associated with location for tid (the identifier of the telemetrielocationid)
            storetimeseries(sid, tid, flagid, sid2)
        elif ogw == "j" and dgw == None:
            pid = sparameter(
                fc,
                "ondiepgrondwater",
                "ondiepgrondwater",
                ("stand", "m-NAP"),
                "ondiepgrondwater",
            )
            sid = sserieskey(fc, pid, lid, fid, fm)
            # store timeseries associated with location for tid (the identifier of the telemetrielocationid)
            storetimeseries(sid, tid, flagid)
        elif dgw == "j" and ogw == None:
            pid = sparameter(
                fc,
                "diepgrondwater",
                "diepgrondwater",
                ("stand", "m-NAP"),
                "ondiepgrondwater",
            )
            sid = sserieskey(fc, pid2, lid, fid, fm)
            # store timeseries associated with location for tid (the identifier of the telemetrielocationid)
            storetimeseries(sid, tid, flagid)

        print("Data for location", nam, "stored in database")


def storetimeseries(sid, id, flagid, sid2=None):
    """With the unique identifier for each location this function calls
       a second rest service to get timeseries data

    Args:
        sid (json): timeseriesid (or better, unique identifier for combination
                    location, parameter, unit)
        id (integer): unique identifier of the location
        flagid (integer): identifier indicating quality of the data
        sid2 (integer): in case of a second filter for the same location a
                        unique identifier describes a different dataset
    """

    # now for each location retrieve the timeseries data.
    # get a list of shortnames, that is the key
    ts = requests.get(
        f"""https://gis.wetterskipfryslan.nl/arcgis/rest/services/Grondwatersite_mpn/MapServer/1/query?outFields=*&orderByFields=MONSTERDATUM&f=json&where=TELEMETRIELOCATIEID='{id}'+AND+MONSTERDATUM+>+date+'2010-01-01+00:00:00'+"""
    )
    print(id, ts.status_code)
    if ts.status_code == 200:
        tsdata = ts.json()
        for v in range(len(tsdata["features"])):
            text = "ondiep"
            tid = None
            if sid2 is not None:
                tid = tsdata["features"][v]["attributes"]["TELEMETRIEKANAALID"]
                if tid == "Ai5":
                    dt = tsdata["features"][v]["attributes"]["MONSTERDATUM"]
                    vl = tsdata["features"][v]["attributes"]["WAARDE"]
                elif tid == "Ai6":
                    dt = tsdata["features"][v]["attributes"]["MONSTERDATUM"]
                    vl = tsdata["features"][v]["attributes"]["WAARDE"]
                    text = "diep"
            else:
                dt = tsdata["features"][v]["attributes"]["MONSTERDATUM"]
                vl = tsdata["features"][v]["attributes"]["WAARDE"]

            date_time_obj = timestamp_to_datetime(dt)
            print(text, id, tid, sid, date_time_obj, vl)
            anid = (
                session.query(tsv)
                .filter_by(timeserieskey=sid, datetime=date_time_obj, scalarvalue=vl)
                .first()
            )
            if anid == None:
                insert = tsv(
                    timeserieskey=sid,
                    datetime=date_time_obj,
                    scalarvalue=vl,
                    flags=flagid,
                )
                session.merge(insert)
                session.commit()


def lastgwstage(engine, gwslocation, t, pid, fid):
    """Retrieves last entrance in the database for the given combination of BROid, filesourckey and paramaterkey

    Args:
        brolocation (string): location of bro_id, incl. filternumber
        pid (integer): parameterkey
        fid (integer): filesourckey
    """
    strsql = f"""
    select max(datetime) from wskip_timeseries.location l
    join wskip_timeseries.timeseries ts on ts.locationkey = l.locationkey
    join wskip_timeseries.parameter p on p.parameterkey = ts.parameterkey
    join wskip_timeseries.filesource f on f.filesourcekey = ts.filesourcekey
    join wskip_timeseries.timeseriesvaluesandflags tsf on tsf.timeserieskey = ts.timeserieskey
    where l.name = '{brolocation}_{t}' and f.filesourcekey = {fid} and p.parameterkey = {pid}
    """
    ld = engine.execute(strsql).fetchall()
    adate = ld[0][0]
    if adate is None:
        strdate = None
    else:
        adate = adate + timedelta(hours=2)
        strdate = adate.strftime("%Y-%m-%d")
        print(brolocation, strdate)
    return strdate


# Function to convert timestamp to datetime
def timestamp_to_datetime(timestamp):
    """Genereates a valid datetemobject from datatime given in seconds

    Args:
        timestamp (integer): julian date in seconds

    Returns:
        datetime: returns valid datetime object
    """
    return datetime.fromtimestamp(timestamp / 1000.0)


if __name__ == "__main__":
    """
    Description
    This routine calls two arcgis rest services from groundwaterportal of Wetterskip Fryslan
    https://www.wetterskipfryslan.nl/kaarten/grondwaterstanden

    Data is stored in a PostgreSQL PostGIS data with a FEWS timeseries data model

    """
    lstlnks = (
        """https://gis.wetterskipfryslan.nl/arcgis/rest/services/Grondwatersite_mpn/MapServer/0/query?returnGeometry=true&where=1=1&outSr=4326&outFields=*&geometry={"xmin":2.8125,"ymin":52.482780222078226,"xmax":5.625,"ymax":54.16243396806779,"spatialReference":{"wkid":4326}}&geometryType=esriGeometryEnvelope&spatialRel=esriSpatialRelIntersects&inSr=4326&geometryPrecision=6&f=json""",
        """https://gis.wetterskipfryslan.nl/arcgis/rest/services/Grondwatersite_mpn/MapServer/0/query?returnGeometry=true&where=1=1&outSr=4326&outFields=*&geometry={"xmin":5.625,"ymin":52.482780222078226,"xmax":8.4375,"ymax":54.16243396806779,"spatialReference":{"wkid":4326}}&geometryType=esriGeometryEnvelope&spatialRel=esriSpatialRelIntersects&inSr=4326&geometryPrecision=6&f=json""",
    )

    for alink in lstlnks:
        print(alink)
        response = requests.get(alink)
        if response.status_code == 200:
            data = response.json()
            # store link as filesource
            fid = loadfilesource(alink, fc, remark="online resource")[0][0]
            # procesgeom loads/checks geometry, parameter, and stores all data
            procesdata(data, fid)
        else:
            print(f"Request failed with status code: {response.status_code}")
