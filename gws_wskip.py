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
# base link is https://gis.wetterskipfryslan.nl/arcgis/rest/services/Grondwatersite_mpn/MapServer/1/query?outFields=*&orderByFields=MONSTERDATUM&f=json&where=TELEMETRIELOCATIEID%3D%27L6206%27+AND+MONSTERDATUM+%3E+date+%272021-8-2+00%3A00%3A00%27+

import os
import requests
from datetime import datetime

from ts_helpders import loadfilesource,location,establishconnection,Location
from ts_helpders import sparameter,stimestep,sflag,sserieskey

local = True
if local:
    fc = r"C:\develop\extensometer\localhost_connection.txt"
else:
    fc = r"C:\projecten\nobv\2023\connection_online.txt"

mapserver_url = 'https://gis.wetterskipfryslan.nl/arcgis/rest/services/Grondwatersite_mpn/MapServer/'
session,engine = establishconnection(fc)
def get_mapserver_data(mapserver_url,id):
    try:
        # Construct the URL for the specific layer's query endpoint
        query_url = f"{mapserver_url}{id}/query"

        # Parameters for the query
        params = {
            "f": "json",         # Format of the response (JSON)
            "where": "1=1",      # Query condition (return all features)
            "outFields": "*",   # Fields to include in the response (all fields)
            "returnGeometry": "true"  # Include geometry in the response
        }

        # Make the HTTP GET request to the MapServer
        response = requests.get(query_url, params=params)

        # Check if the request was successful (HTTP status code 200)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Request failed with status code: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return None

def procesgeom(jsonrespons,fid):
    features = jsonrespons['features']
    for i in range(len(features)):
        attrib = features[i]['attributes']
        loc = attrib['LOCATIE']
        nam = attrib['NAAM']
        ogw = attrib['ONDIEPGRONDWATER']
        dgw = attrib['DIEPGRONDWATER']
        fm  = attrib['FREQUENTIEMETING']
        tid = attrib['TELEMETRIELOCATIEID']
        des = attrib['OMSCHRIJVING']
        mv  = attrib['MAAIVELD']
        x = jsonrespons['features'][i]['geometry']['x']
        y = jsonrespons['features'][i]['geometry']['y']
        lid = location(fc,fid[0][0],nam,x,y,epsg=4326,shortname=tid,description=des,z=mv)
        
        # set the flag key to validated
        flagid = sflag(fc,'validated')

        # set the timestep
        tstid = stimestep(fc,fm,'dagelijks')

        # set the parameterkey and the serieskey
        ogw = 'n'
        dgw = 'n'
        if ogw == 'j':
            pid = sparameter(fc,'ondiepgrondwater','ondiepgrondwater',('stand','m-NAP'))
            sid = sserieskey(fc,pid,lid,fid,fm)
        if dgw == 'j':
            pid = sparameter(fc,'diepgrondwater','diepgrondwater',('stand','m-NAP'))
            sid = sserieskey(fc,pid,lid,fid,fm)

        # there is 1 tube with two filters
        # deep is Ai6 and Ai5
        print('locatie',nam,'opgeslagen in database')
        return lid

# Function to convert timestamp to datetime
def timestamp_to_datetime(timestamp):
    return datetime.fromtimestamp(timestamp / 1000.0)


if __name__ == "__main__":
    # Example usage
    lstlnks = ("""https://gis.wetterskipfryslan.nl/arcgis/rest/services/Grondwatersite_mpn/MapServer/0/query?returnGeometry=true&where=1=1&outSr=4326&outFields=*&geometry={"xmin":2.8125,"ymin":52.482780222078226,"xmax":5.625,"ymax":54.16243396806779,"spatialReference":{"wkid":4326}}&geometryType=esriGeometryEnvelope&spatialRel=esriSpatialRelIntersects&inSr=4326&geometryPrecision=6&f=json""",
               """https://gis.wetterskipfryslan.nl/arcgis/rest/services/Grondwatersite_mpn/MapServer/0/query?returnGeometry=true&where=1=1&outSr=4326&outFields=*&geometry={"xmin":5.625,"ymin":52.482780222078226,"xmax":8.4375,"ymax":54.16243396806779,"spatialReference":{"wkid":4326}}&geometryType=esriGeometryEnvelope&spatialRel=esriSpatialRelIntersects&inSr=4326&geometryPrecision=6&f=json""")

    for alink in lstlnks:
        response = requests.get(alink)
        if response.status_code == 200:
            data = response.json()
            # store link as filesource
            fid = loadfilesource(alink,fc,remark='online resource')
            lid = procesgeom(data,fid)
        else:
            print(f"Request failed with status code: {response.status_code}")
    

    # now for each location retrieve the timeseries data.
    # get a list of shortnames, that is the key
    tmpl = f"""https://gis.wetterskipfryslan.nl/arcgis/rest/services/Grondwatersite_mpn/MapServer/1/query?outFields=*&orderByFields=MONSTERDATUM&f=json&where=TELEMETRIELOCATIEID='{id}'+AND+MONSTERDATUM+>+date+'2023-01-01+00:00:00'+"""
    records = session.query(Location).all()
    for r in records:
        id = r.shortname
        ts = requests.get(f"""https://gis.wetterskipfryslan.nl/arcgis/rest/services/Grondwatersite_mpn/MapServer/1/query?outFields=*&orderByFields=MONSTERDATUM&f=json&where=TELEMETRIELOCATIEID='{id}'+AND+MONSTERDATUM+>+date+'2010-01-01+00:00:00'+""")
        if ts.status_code == 200:
            tsdata = ts.json()
            print(id, len(tsdata['features']))
            for v in range(len(tsdata['features'])):
                id = tsdata['features'][v]['attributes']['TELEMETRIELOCATIEID']
                dt = tsdata['features'][v]['attributes']['MONSTERDATUM']
                date_time_obj = timestamp_to_datetime(dt)
                vl = tsdata['features'][v]['attributes']['WAARDE']
                print(id,date_time_obj,vl)


