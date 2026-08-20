* NOTE gm_tasks.list contains the key (region code) list for the regions that we want to process
* NOTE gm_polygons.geojson contains the geometry (coordinates) of these regions (looked up by that key)
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
- (/src) python esa_gm.py
* NOTE LS: run for 1 month, m7a.8xlarge, 128G RAM, 32 CPUs. 4GB input when COG'd. About 12 minutes.
* NOTE LS: run for 1 year, r6a.48xlarge, 1.5T RAM, 192 CPUs. About 150 minutes.
* NOTE S2 more pixels: run for 1 month, m7a.8xlarge, 128G RAM, 32 CPUs. about 50% MEM usage. About 1 hour.
* NOTE S2 cleaner: run for 1 month, m7a.8xlarge, 128G RAM, 32 CPUs. about 50% MEM usage. About 40 minutes.
* NOTE S2 cleaner: run for 6 months, r6a.48xlarge, 1.5T RAM, 192 CPUs. about 30% MEM usage. About 80 minutes.
* NOTE S2 cleaner: run for 6 months, r6a.48xlarge, 1.5T RAM, 192 CPUs. about 30% MEM usage. About 100 minutes.
* NOTE S2 10m 4 bands: run for 1 month, m7a.8xlarge, 128G RAM, 32 CPUs. Killed.
* NOTE S2 10m 4 bands: run for 1 month, r6a.48xlarge, 1.5T RAM, 192 CPUs. mem at 20%. not good. 33 minutes. $USD 6.50
* NOTE S2 10m 4 bands: 1 month, m7a.16xlarge, 64 CPUs, 256G RAM. 50 minutes. $USD 4.63, (500,500) chunks, about 70% mem
