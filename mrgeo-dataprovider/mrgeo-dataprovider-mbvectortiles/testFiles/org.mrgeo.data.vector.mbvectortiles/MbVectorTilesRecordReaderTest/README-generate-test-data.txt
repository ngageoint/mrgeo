The mbtiles test data for the Mapbox vector tiles unit tests were generated from
some existing test data. Use the Mapbox project called tippecanoe to convert
from GeoJSON into mb vector tiles. The following command shows how to do that:

tippecanoe -z 0 -Z 0 -o simple-triangles-z0.mbtiles simple-triangles.json


If existing source data is not GeoJSON, then use ogr2ogr to convert into GeoJSON:

ogr2ogr -f GeoJSON simple-triangles.json simple-triangles.shp
