"""
Declaration via ORM mapping of KINM data, including FEWS time series
compatibla datamodel, requires:
Pyhton packages
 - sqlalchemy
 - geoalchemy2
PostgreSQL/PostGIS
 - schema hhnk_timeseries
"""

#  Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2022 Deltares for Projects with a FEWS datamodel in
#                 PostgreSQL/PostGIS database used in Water Information Systems
#   Gerrit Hendriksen@deltares.nl
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

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Sequence,
    ForeignKey,
    Column,
    UniqueConstraint,
    PrimaryKeyConstraint,
)
from sqlalchemy import Integer, Float, DateTime, String, Text
from geoalchemy2 import Geometry
from sqlalchemy.dialects import postgresql

Base = declarative_base()

"""
FEWS data model has been partially adopted. Due to completion of the datamodel with borehole datamodel the timeseries datamodel is split in 
several parts. A common part of the datamodel (location, filesource, parameter and unit tables) are now considered as shared tables over various schemas
"""


class TimeStep(Base):
    __tablename__ = "timesteps"
    __table_args__ = {"schema": "hhnk_timeseries"}
    timestepkey = Column(
        Integer, Sequence("hhnk_timeseries.timesteps_timestepkey_seq"), primary_key=True
    )
    id = Column(String, unique=True, nullable=False)
    label = Column(String)


class TimeSeries(Base):
    __tablename__ = "timeseries"
    __table_args__ = {"schema": "hhnk_timeseries"}
    timeserieskey = Column(
        Integer,
        Sequence("hhnk_timeseries.timeserieskeys_timeserieskey_seq"),
        primary_key=True,
    )
    locationkey = Column(
        Integer, ForeignKey("hhnk_timeseries.location.locationkey", ondelete="CASCADE")
    )
    parameterkey = Column(
        Integer,
        ForeignKey("hhnk_timeseries.parameter.parameterkey", ondelete="CASCADE"),
    )
    timestepkey = Column(
        Integer, ForeignKey("hhnk_timeseries.timesteps.timestepkey", ondelete="CASCADE")
    )
    filesourcekey = Column(
        Integer,
        ForeignKey("hhnk_timeseries.filesource.filesourcekey", ondelete="CASCADE"),
    )
    valuetype = Column(Integer, nullable=False, default=0)
    modificationtime = Column(DateTime, nullable=False)


class TimeSeriesComments(Base):
    __tablename__ = "timeseriescomments"
    __table_args__ = {"schema": "hhnk_timeseries"}
    timeserieskey = Column(
        Integer,
        ForeignKey("hhnk_timeseries.timeseries.timeserieskey", ondelete="CASCADE"),
        primary_key=True,
    )
    datetime = Column(DateTime, primary_key=True)
    commenttext = Column(String)


class Users(Base):
    __tablename__ = "users"
    __table_args__ = {"schema": "hhnk_timeseries"}
    userkey = Column(
        Integer, Sequence("hhnk_timeseries.users_userkey_seq"), primary_key=True
    )
    id = Column(String, unique=True, nullable=False)
    name = Column(String)


class Flags(Base):
    __tablename__ = "flags"
    __table_args__ = {"schema": "hhnk_timeseries"}
    flagkey = Column(
        Integer, Sequence("hhnk_timeseries.users_flagkey_seq"), primary_key=True
    )
    id = Column(String, unique=True, nullable=False)
    name = Column(String)


class TimeSeriesManualEditsHistory(Base):
    __tablename__ = "timeseriesmanualeditshistory"
    __table_args__ = {"schema": "hhnk_timeseries"}
    timeserieskey = Column(
        Integer,
        ForeignKey("hhnk_timeseries.timeseries.timeserieskey", ondelete="CASCADE"),
        primary_key=True,
    )
    editdatetime = Column(DateTime, primary_key=True)
    datetime = Column(DateTime, primary_key=True)
    userkey = Column(
        Integer, ForeignKey("hhnk_timeseries.users.userkey", ondelete="CASCADE")
    )
    scalarvalue = Column(Float)
    flags = Column(Integer, nullable=False)
    commenttext = Column(String)


class TimeSeriesValuesAndFlags(Base):
    __tablename__ = "timeseriesvaluesandflags"
    __table_args__ = (
        PrimaryKeyConstraint("timeserieskey", "datetime", "scalarvalue"),
        {"schema": "hhnk_timeseries"},
    )
    timeserieskey = Column(
        Integer,
        ForeignKey("hhnk_timeseries.timeseries.timeserieskey", ondelete="CASCADE"),
        nullable=False,
    )
    datetime = Column(DateTime, nullable=False)
    scalarvalue = Column(Float, nullable=False)
    flags = Column(
        Integer,
        ForeignKey("hhnk_timeseries.flags.flagkey", ondelete="CASCADE"),
        nullable=False,
    )


"""records pump history"""


class Parameter(Base):
    __tablename__ = "parameter"
    __table_args__ = {"schema": "hhnk_timeseries"}
    parameterkey = Column(
        Integer,
        Sequence("hhnk_timeseries.parameter_parameterkey_seq"),
        primary_key=True,
    )
    id = Column(String)
    name = Column(String)
    unitkey = Column(
        Integer, ForeignKey("hhnk_timeseries.unit.unitkey", ondelete="CASCADE")
    )
    compartment = Column(String)
    shortname = Column(String)
    description = Column(String)
    valueresolution = Column(Float)
    waarnemingssoort = Column(String)


class Unit(Base):
    __tablename__ = "unit"
    __table_args__ = {"schema": "hhnk_timeseries"}
    unitkey = Column(
        Integer, Sequence("hhnk_timeseries.unitdesc_seq"), primary_key=True
    )
    unit = Column(String, nullable=False)
    unitdescription = Column(String)


class FileSource(Base):
    __tablename__ = "filesource"
    __table_args__ = {"schema": "hhnk_timeseries"}
    filesourcekey = Column(
        Integer, Sequence("hhnk_timeseries.filesource_seq"), primary_key=True
    )
    deviceid = Column(String)
    filesource = Column(String, nullable=False)
    remark = Column(String)
    lasttransactionid = Column(Integer)

    def __repr__(self):
        return "<FileSource: filesourcekey=%s, lasttransactionid=%s) >" % (
            self.filesourcekey,
            self.lasttransactionid,
        )


class Transaction(Base):
    __tablename__ = "transaction"
    __table_args__ = {"schema": "hhnk_timeseries"}
    transactionkey = Column(
        Integer, Sequence("hhnk_timeseries.transaction_seq"), primary_key=True
    )
    timeserieskey = Column(
        Integer,
        ForeignKey("hhnk_timeseries.timeseries.timeserieskey", ondelete="CASCADE"),
        nullable=False,
    )
    transactiontime = Column(DateTime, nullable=False)
    periodstart = Column(DateTime, nullable=False)
    periodend = Column(DateTime, nullable=False)
    transactionid = Column(Integer, nullable=False)


class Location(Base):
    __tablename__ = "location"
    __table_args__ = {"schema": "hhnk_timeseries"}
    locationkey = Column(
        Integer, Sequence("hhnk_timeseries.location_locationkey_seq1"), primary_key=True
    )  # key
    filesourcekey = Column(
        Integer,
        ForeignKey("hhnk_timeseries.filesource.filesourcekey", ondelete="CASCADE"),
    )  # fskey
    diverid = Column(String)
    filterid = Column(Integer)
    filterdepth = Column(Float)
    name = Column(String)
    shortname = Column(String)
    description = Column(String)
    x = Column(Float)
    y = Column(Float)
    z = Column(Float)
    epsgcode = Column(Integer)
    geom = Column(Geometry("POINT", srid=28992))
    altitude_msl = Column(Float)  # hoogte maaiveld
    tubetop = Column(Float)
    tubebot = Column(Float)
    cablelength = Column(Float)  # kabellengte

    def __repr__(self):
        return "<Location: locationkey=%s)>" % (self.locationkey)
