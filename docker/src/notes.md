* NOTE gm_tasks.list contains the key (region code) list for the regions that we want to process
* NOTE gm_polygons.geojson contains the geometry (coordinates) of these regions (looked up by that key)
* NOTE do not assume GA SSO role, the assumed devbox role should work
* DONE build docker image from fork
* DONE modify gm_polygons.geojson to report the location of the Gold Ridge mine path/row
* DONE grab the coordinates from the world path/row geojson file, put it in gm_polygons.geojson
* DONE fix the date
* TODO run the docker image
- (odc-stats) docker build -t usgsgm:dev .
- (working-dir)
- mkdir output
- make up
- make bash
- (/src) python usgs_gm.py
* NOTE run for 1 month, m7a.8xlarge, 128G RAM, 32 CPUs. 4GB input when COG'd.