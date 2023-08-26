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

for each of the tables the basic input is described in coming paragraphs.

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