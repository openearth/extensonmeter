# Extensometer

Although called Extensometer is has become a more general setup of timeseries data on behalf of research for soil subsidence.

This github contains code for retrieving subsidence data from sftp server and putting it in a postgreSQL database.

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
