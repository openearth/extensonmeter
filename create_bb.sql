drop table timeseries.boundingbox;
create table timeseries.boundingbox as
SELECT st_makeenvelope(min(st_x(geom)), min(st_y(geom)),max(st_x(geom)), max(st_y(geom)),28992),
left(name,3) as groupname,
min(st_x(geom)) as minx,
min(st_y(geom)) as miny,
max(st_x(geom)) as maxx,
max(st_y(geom)) as maxy 
FROM timeseries.location
group by left(name,3)