# Extensometer

Although called Extensometer is has become a more general setup of timeseries data on behalf of research on soil subsidence.

This github contains code for retrieving subsidence data from sftp server and putting it in a postgreSQL database.

Below this the datamodel is described as well as possible. 
The first section is about the datasource with specific attention to the units.

# Datasources
As much as 6 datasources are used derive around 1760 filters. Not all these filters are equally interesting and a specific subset is used for the calibration.

The datasources are described in following table with the unit of the filtersetting as well as the unit of the timeseries data.

|bron|eenheid|type|opmerking|
|---|---|---|---|
HDSR API|meter|Nap|API|	|
HHNK API|meter NAP|API|	|
NOBV data|Meter maaiveld|file||	
Handmatige data|Meter maaiveld|file	
BRO	meter|NAP|API|Hydropandas package|
WSKIP|meter NAP|dataservice|niet een echte API|


# Datamodel
The data model used is the DDL construction of FEWS Open databases. This datamodel is adapted to the needs of groundwater specific data, such as tube depth etc. 

The data model constist of various supporting tables and several base tables. The most important tables are:
- location
- parameter
- unit 
- filesource
- flags
- timesteps

For each of the tables the basic input is described in coming paragraphs.
The DDL is setup in SLQAlchemy ORM setup. The DDL is described in orm_timeseries.py and loaded into via orm_loadtimeseries.py

Bear in mind that the CRS (Coordinate Reference System) should be known in advance and cannot have mixed CRS. 

## filesource
This table is not described here as it is part of the various ways of loading data into the timeseries tables. The table is loaded with references to files/sources in order track the source of eacht entry in timeseriesvalues and locations.

## Location
The location table describes the location of a measurement. This has only unique descriptions of each filter. 

|columnname|data type|obligation|description|
|---|---|---|---|
|locationkey        |Integer|na| automatically generated key|
|filesourcekey      |Integer|na| automatically generated key|
|diverid            |String |mandatory|diver identification|
|filterid           |Integer|mandatory|diver number (should be number)|
|filterdepth        |Float  |mandatory|filter depth of filter (please describe in description reference level)|
|name               |String |mandatory|tube identification|
|shortname          |String |not mandatory|free text|
|description        |String |not mandatory|all relevant information, such as used metrics|
|x                  |Float  |mandatory| x coordinate|
|y                  |Float  |mandatory| y coordinate|
|z                  |Float  |not mandatory| z coordinate of filter if tubetop and tubebot are not known|
|epsgcode           |Integer|mandatory| EPSG code (see [epsg.io](https://www.epsg.io/)) |
|geom               |Geometry| na|automatic generated geographic object|
|altitude_msl       |Float|not mandatory| surface level  |
|tubetop            |Float|not mandatory| top of filter|
|tubebot            |Float|not mandatory| bottom of filter|
|cablelength        |Float|not mandatory| lenght of cable|

## Parameter
Parameter description is a table with unique entries. The combination parameter and unit should be described in relation to each other. If there are mixed parameter/unit combinations then these will be treated as two distinct entities.

|columnname|data type|obligation|description|
|---|---|---|---|
|parameterkey       |Integer|na|automatically generated key|
|id                 |String|mandatory |preferably standardized code according to any standard (if possible refer to the url)|
|name               |String|mandatory |preferably standardized name according to any standard (if possible refer to the url)|
|unitkey            |Integer|na |automatically generated with loading parameter and unit|
|compartment        |String|not mandatory |preferably standardized name of the compartment according to any standard |
|shortname          |String|not mandatory |any name|
|description        |String|not mandatory |any description for better understanding the data|
|valueresolution    |Float|not mandatory |any comment |
|waarnemingssoort   |String|not mandatory |any comment|

## unit 
Like parameter description a unique set of entries related to parameter. 

|columnname|data type|obligation|description|
|---|---|---|---|
|unitkey            |Integer|na |automatically generated key|
|unit               |String|mandatory | unitcode in si units|
|unitdescription    |String|mandatory | unitdescription in si description|

## flags
The flags table is a table with flags in case of any QA/QC. In case the data is not flagged with any quality parameter it is recommendet to define a value (raw data for instance). Any well described value increases later use of the data.

|columnname|data type|obligation|description|
|---|---|---|---|
|flagkey        |Integer|na |automatically generated|
|id             |String|mandatory |identifier idicating the QA/QC of the observations done|
|name           |String|mandatory |description of the QA/QC|

## timesteps
Default value is often set to nonequisistant. Any other values will increase later use of the data.
Make sure that the format is well known with the processing engineer who is assigned with the task of loading the data. Preferably the datetime object is in [ISO8601](https://www.iso.org/iso-8601-date-and-time-format.html) format. 

|columnname|data type|obligation|description|
|---|---|---|---|
|timeserieskey  |Integer|na |automatically generated code|
|datetime       |DateTime|mandatory |ISO8601 standardised datetime object|
|commenttext    |String|not mandatory |any description that will increase usage|

## calibration database
All data from subsequent providers will be aggregated into tables GWM and SWM, which are Groundwatermonitoring and surfacewatermonitoring respectively.

GWM table setup 
|columnname|data type|
|---|---|
source|text 
well_id|text 
aan_id|text 
transect|text 
parcel_type|text 
x_centre_parcel|double precision 
y_centre_parcel|double precision 
soil_class|text 
surface_level_m_nap|double precision 
parcel_width_m|double precision 
summer_stage_m_nap|double precision 
winter_stage_m_nap|double precision 
ditch_id|text 
x_well|double precision 
y_well|double precision 
distance_to_ditch_m|double precision 
distance_to_road_m|double precision 
distance_to_railroad_m|double precision 
distance_to_wis_m|double precision 
start_date|text 
end_date|text 
records|integer 
trenches|ARRAY 
trench_depth_m_sfl|double precision 
wis_distance_m|double precision 
wis_depth_m_sfl|double precision 
ditch_name|text 
veenperceel|boolean 
name|text 
filterdepth|double precision 
source|integer 
well_id|text 
aan_id|text 
name|text 
transect|text 
parcel_type|text 
ditch_id|text 
ditch_name|text 
soil_class|text 
surface_level_m_nap|double precision 
start_date|text 
end_date|text 
parcel_width_m|double precision 
summer_stage_m_nap|double precision 
winter_stage_m_nap|double precision 
x_well|double precision 
y_well|double precision 
distance_to_ditch_m|double precision 
trenches|ARRAY 
trench_depth_m_sfl|double precision 
wis_distance_m|double precision 
wis_depth_m_sfl|double precision 
tube_top|double precision 
tube_bot|double precision 
geometry|USER-DEFINED 
parcel_geom|text 
selection|text 
description|text 

