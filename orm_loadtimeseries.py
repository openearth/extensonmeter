"""
Declaration via ORM mapping of Subsurface datamodel, including FEWS time series
compatibla datamodel, requires:
Pyhton packages
 - sqlalchemy
 - geoalchemy2
PostgreSQL/PostGIS
 - schema fews
 - schema borehole
"""

#  Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2021 Deltares for Projects with a FEWS datamodel in 
#                 PostgreSQL/PostGIS database used in Water Information Systems
#   Gerrit Hendriksen@deltares.nl
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

## Connect to the DB

from sqlalchemy import create_engine

## Declare a Mapping to the database
from orm_timeseries_hhnk import Base

def checkschema(engine,schema):
    strsql = 'create schema if not exists {s}'.format(s=schema)
    engine.execute(strsql)

def readcredentials(fc):
    f = open(fc)
    engine = create_engine(f.read(), echo=False)
    #engine = create_engine("postgresql+psycopg2://postgres:ghn13227@localhost/nobv")
    f.close()
    return engine

# function to create the database, bear in mind, only to be executed when first started
def createdb(engine):
    ## Create the Table in the Database
    Base.metadata.create_all(engine)

# drop (delete) database
def dropdb(engine):
    Base.metadata.drop_all(engine)

def resetindex(engine,schema):
    strsql = """SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = 'hdsrtimeseries' ORDER BY tablename,indexname;"""
    lst = engine.execute(strsql)
    for i in lst:
        print(i.tablename,i.indexname)
        strSql = """ALTER SEQUENCE hdsrtimeseries.{indx} RESTART WITH 1""".format(indx=i.indexname)
        engine.execute(strSql)
        
if __name__ == "__main__":
    local = True
    if local:
        fc = r"C:\projecten\grondwater_monitoring\nobv\2023\connection_local_somers.txt"
    else:
        fc = r"C:\projecten\grondwater_monitoring\nobv\2023\connection_online_qsomers.txt"
    engine = readcredentials(fc)
    #when multiple schemas
    # schemas = ('subsurface_second')
    # for schema in schemas:
    #     checkschema(engine,schema)
    lschema = ('hhnktimeseries',)
    for schema in lschema:
        checkschema(engine,schema)
    # format is #postgres://user:password@hostname/database (in this case hydrodb)    
    dropdb(engine)    
    createdb(engine) # bear in mind deletes database before creation


