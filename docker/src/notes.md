* NOTE gm_tasks.list contains the key (region code) list for the regions that we want to process
* NOTE gm_polygons.geojson contains the geometry (coordinates) of these regions (looked up by that key)
* TODO build docker image from fork
* TODO modify gm_polygons.geojson to report the location of the Gold Ridge mine path/row
* TODO grab the coordinates from the world path/row geojson file, put it in gm_polygons.geojson
* TODO fix the date
* TODO run the docker image
- (odc-stats) docker build -t usgsgm:dev .
- (working-dir)
- mkdir output
- make up
- make bash
- (/src) python usgs_gm.py