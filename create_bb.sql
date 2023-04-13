drop table timeseries.boundingbox;
create table timeseries.boundingbox as
SELECT st_makeenvelope(min(st_x(l.geom)), min(st_y(l.geom)),max(st_x(l.geom)), max(st_y(geom)),28992) as bbgeom,
left(name,3) as groupname,
min(st_x(l.geom)) as minx,
min(st_y(l.geom)) as miny,
max(st_x(l.geom)) as maxx,
max(st_y(l.geom)) as maxy 
FROM timeseries.location l
join timeseries.timeseries ts on ts.locationkey = l.locationkey
where l.geom is not Null
group by left(name,3)