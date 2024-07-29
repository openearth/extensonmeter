# -*- coding: utf-8 -*-
"""
Created on Thu Jan  7 14:49:58 2021

@author: hendrik_gt
"""

#  Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2021 Deltares for KINM (KennisImpuls Nutrienten Maatregelen)
#   Gerrit.Hendriksen@deltares.nl
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

# system modules
import os
import time
import datetime

# third party modules
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy  import create_engine,func, update, insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy  import Boolean, Integer, Float, DateTime, String, Text
from geoalchemy2 import Geometry

## Declare a Mapping to the database
from orm_timeseries_delf import Base, FileSource, Location, Parameter, Unit, TimeSeries, TimeStep, Flags, Transaction

def establishconnection(fc):
    """
    Set up a orm session to the target database with the connectionstring
    in the file that is passed

    Parameters
    ----------
    fc : string
        DESCRIPTION.
        Location of the file with a connectionstring to a PostgreSQL/PostGIS
        database

    Returns
    -------
    session : ormsession
        DESCRIPTION.
        returns orm session

    """
    f = open(fc)
    engine = create_engine(f.read(), echo=False)
    f.close()
    Session = sessionmaker(bind = engine)
    session = Session()
    session.rollback()
    return session,engine

def loadfilesource(source,fc, remark='',lasttransactionid=None):
    """
    Checks whether a source is already recorded in the database. If not recorded,
    then creates a new entry. In any case the filesourcekey is returned.

    Parameters
    ----------
    source : string 
        string with source (can be filesource or link to online api)
    deviceid : integer
        unique identifier provided by the manufacturer, specifically introduced for trailer ids
    remark : string
        any arbitrary remark
    fc : string
        link to file with connection string to database

    Returns
    -------
    filesourcekey of the record entered or found

    """
    
    # setup connection to database
    session,engine = establishconnection(fc)
    
    f = session.query(FileSource).filter_by(filesource=source).first()
    try:
        if str(f) == 'None':
            f = FileSource(filesource=source,remark=remark,lasttransactionid=lasttransactionid)
            session.add(f)
            session.commit()
        fkey = f.filesourcekey,
        ftid = f.lasttransactionid
    except:
        fkey = None
        ftid = None
        print("exception raised while retrieving/assigning filesource")
    finally:            
        session.close()
        engine.dispose()
        return fkey,ftid

def location(fc,fskey,name,x,y,epsg,shortname='',description='',z=0,altitude_msl=0, diverid=None,tubebot=None,tubetop=None):
    """
    Parameters
    ----------
    fc : String

        Link to credentials file for access to database.
    fskey : integer
        Foreignkey value that is the link to the filesource or online source of the information.
    name : string
        Name for the location.
    shortname : string
        Alternative name for the location.
    description : string
        Description of the location.
    x : float
        x or long coordinate.
    y : float
        y or lat coordinate.
    z : float
        eleveation of the installation with respect to reference (i.e. m-MSL or m-NAP).
    epsg : integer
        integer value corresponding to global coordinate reference sytems (i.e. NL = RDNAP = 28992, WGS84=4326)
    altitude_msl : float
        eleveation area of the installation with respect to reference (i.e. m-MSL or m-NAP).

    Returns
    -------
    Id of the location

    """
    # setup connection to database
    if fskey==None:
        print('please give filesourcekey, this is required')
        return
    if x==None or y==None or epsg==None:
        print('please provide coordinates and a proper crs epsg code, these are required')
        return
    else:
        print(x,y,epsg)
    session,engine = establishconnection(fc)
    f = session.query(Location).filter_by(name=name).first()
    try:
        if str(f) == 'None':
            f = Location(filesourcekey=fskey,
                 name=name,
                 shortname=shortname,
                 description=description,
                 tubebot=tubebot,
                 tubetop=tubetop,
                 x=x,
                 y=y,
                 z=z,
                 epsgcode=epsg,
                 diverid=diverid,
                 altitude_msl=altitude_msl,
                 )
            
            session.add(f)
            session.commit()
            if epsg == 28992:
                strsql = 'update delftimeseries.location set geom = st_setsrid(st_point(x,y),epsgcode) where geom is null'
            else:
                strsql = 'update delftimeseries.location set geom = st_transform(st_setsrid(st_point(x,y),epsgcode),28992) where geom is null'
            engine.execute(strsql)
        else:
            print('name already stored in location table', name, f.locationkey)
        lkey = f.locationkey
    except:
        lkey = None
        print("exception raised while retrieving/assigning location")
    finally:
        session.close()
        engine.dispose()
        return lkey        

def sunit(fc,unit,descr):
    """
    Parameters
    ----------
    fc : string
        Link to credentials file for access to database..
    unit : string
        short unit description (mg/l).
    descr : string
        long unit description (milligram per liter in surface water).

    Returns
    -------
    unitkey: integer

    """
    session,engine = establishconnection(fc)
    f = session.query(Unit).filter_by(unit=unit).first()
    try:    
        if str(f) == 'None':
           f = Unit(unit=unit,
                    unitdescription=descr)
           session.add(f)
           session.commit()
           unitkey = f.unitkey
        else:
            print('unit already stored in parameter table', unit, f.unitkey)
            unitkey= f.unitkey
    except:
        print("exception raised while retrieving/assigning unit")
        unitkey = None
    finally:
        session.close()
        engine.dispose()
        return unitkey

def sflag(fc,flag,descr=''):
    """
    Parameters
    ----------
    fc : string
        Link to credentials file for access to database..
    flag : string
        indicator for quality of the measurment.
    descr : string
        long unit description (milligram per liter in surface water).

    Returns
    -------
    flagkey: integer

    """
    session,engine = establishconnection(fc)
    f = session.query(Flags).filter_by(id=flag).first()
    try:
        if str(f) == 'None':
           f = Flags(id=flag,
                    name=descr)
           session.add(f)
           session.commit()
        else:
            print('flag already stored in parameter table', flag, f.flagkey)
        flagkey = f.flagkey
    except:
        print("exception raised while retrieving/assigning flag")
        flagkey = None
    finally:
        session.close()
        engine.dispose()
        return flagkey

def sparameter(fc,parameter,name,unit,description,parametergroup=None,shortname=None,valueresolution=None,compartment=None,wns=None):
    """
    Parameters
    ----------
    fc : sting
        Link to credentials file for access to database.
    parameter : string
        Short notation of the parameter (P for Phosphate for instance).
    name : string
        Long name for parameter (Phosphate for instance).
    unit : list of string (unit, unitname)
        Integer derived from unit table.
    description : string
        Addistional description for the parameter.
    shortname : string, optional
        An alternative short description, similar to aquo code! check https://www.aquo.nl/index.php/Id-471174de-c041-2d35-103a-1e0f6f55bd87 . The default is None.
    valueresolution : float, optional
        ?. The default is None.
    compartment : String, optional
        Compartment describes the compartment of the measurement
    waarnemingssoort : String, optional
        Waarnemingssoort is de aquo codering voor de combinatie van typering, grootheid, parameter, hoedanigheid, eenheid en compartiment (bijna)
    Returns
    -------
    parameterkey: integer

    """
    session,engine = establishconnection(fc)
    
    # check or set unit
    unitkey = sunit(fc,unit[0],unit[1])
    f = session.query(Parameter).filter_by(id=parameter,unitkey=unitkey,compartment=compartment,waarnemingssoort=wns).first()
    try:
        if str(f) == 'None':
           f = Parameter(id=parameter,
                         shortname=shortname,
                         description=description,
                         name=name,
                         unitkey=unitkey,
                         valueresolution=valueresolution,
                         compartment=compartment,
                         waarnemingssoort=wns)
           session.add(f)
           session.commit()
           parameterkey = f.parameterkey
        else:
            parameterkey = f.parameterkey
            print('parameter already stored in parameter table', name, f.parameterkey,f.unitkey,f.compartment)
    except:
        print("exception raised while retrieving/assigning parameter")
        parameterkey = None
    finally:        
        session.close()
        engine.dispose()
        return parameterkey

def stimestep(session,timestep,label=''):
    """
    Parameters
    ----------
    session : sqlalchemy sessionmaker object
        sqlalchemy sessionmaker object.
    timestep : string
        timestep identifier.
    label: string
        label of the timestep i.e. description
    Returns
    -------
    tstkey : integer
        timestepkey, unique identifier
    """
    f = session.query(TimeStep).filter_by(id=timestep).first()
    try:
        if str(f) == 'None':
            f = TimeStep(id=timestep,label=label)
            session.add(f)
            session.commit()
        else:
            print('timestep described in table', timestep, f.timestepkey)
        timestepkey = f.timestepkey
    except:
        timestepkey = None
        print("exception raised while retrieving/assigning timestep")
    finally:
        return timestepkey

def sserieskey(fc,parameterkey,locationkey,filesourcekey,timestep='nonequidistant'):
    """
    

    Parameters
    ----------
    fc : string
         Link to credentials file for access to database.
    parameter : integer
        paramterkey (the unique identifier of the paramter).
    locationkey : integer
        the unique location identifier
    filesourcekey : TYPE
        the unique filesource identifier
    timestep : string
        timestep id (the main name of the timestep, default is nonequidistant).

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    session,engine = establishconnection(fc)
    lk = locationkey
    pk = parameterkey
    tk = stimestep(session,timestep)
    fk = filesourcekey
    tsk = session.query(TimeSeries).filter_by(locationkey=lk,parameterkey=pk,timestepkey=tk,filesourcekey=fk).first()
    try:
        if tsk is None:
            # find serieskey max value
            i=session.query(func.max(TimeSeries.timeserieskey).label('timeserieskey')).one().timeserieskey
            if i is None:
                i = 1
            else:
                i=i+1
            atsk = TimeSeries(timeserieskey=i,locationkey=lk,parameterkey=pk,timestepkey=tk,filesourcekey=fk,modificationtime=datetime.datetime.now())
            session.add(atsk)
            session.commit()
            tsk = atsk
        timeserieskey = tsk.timeserieskey
    except:
        timeserieskey = None
        print('exception raised while retrieving/assigning timeseries')            
    finally:
        session.close()
        engine.dispose()
        return timeserieskey

    
def read_config(configfile):
    """
    Reads config file with connection strings to M2Web api

    Parameters
    ----------
    connectionfile : TYPE
        contains connection strings to web api
        
        Structure of the file should be:
            [m2web]
            m2wdevid = (should be derive via https://developer.ewon.biz/content/ewon-programming)
            m2wtoken = (is provided by data provider)

    Returns
    -------
    m2wdevid as string
    m2wtoken as string

    """
    import configparser
    
	# Parse and load
    if not os.path.isfile(configfile):
        print('Please give valid path for ',configfile)
        return None
    else:    
        cf = configparser.ConfigParser() 
        cf.read(configfile)
        return cf

def settransaction(timeserieskey,periodstart,periodend,transactionid,session):
    """
    Used to record the period and transactionid

    Parameters
    ----------
    timeserieskey : integer
        timeserieskey
    periodstart : DateTime
        start date of the set of data retrieved for specific stransactionid
    periodend : DateTime
        start date of the set of data retrieved for specific stransactionid.
    transactionid : Integer
        the transactionid
    session : SqlAlchemy session object
        the current session
    Returns
    -------
    None.

    """
    transactiontime = datetime.datetime.now()
    stmt = Transaction(timeserieskey=timeserieskey,periodstart=periodstart,periodend=periodend,transactiontime=transactiontime,transactionid=transactionid)
    session.add(stmt)
    session.commit()
    
def convertlttodate(lt,ddapi=False):
    """
    Parameters
    ----------
    lt : integer
        integer representation of time.

    Returns
    -------
    adate : datetime object
        return gregorian date.

    """
    if ddapi:
        adate = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(lt / 1000.0))
    else:
        adate = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(lt / 1000.0))
    return adate

def dateto_integer(dt_time,ddapi=False):
    """
    Parameters
    ----------
    dt_time : datetime
        date time object.
    ddapi : boolean
        ddapi = True means that DD-API is called, DD-API demands dateformat with T and Z
    Returns
    -------
    TYPE: integer
        returns integer representation of time.

    """
    if ddapi:
        return time.mktime(time.strptime(dt_time, "%Y-%m-%dT%H:%M:%SZ")) * 1000    
    else:
        return time.mktime(time.strptime(dt_time, "%Y-%m-%d %H:%M:%S")) * 1000    
    