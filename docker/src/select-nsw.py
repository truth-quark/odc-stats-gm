from shapely.geometry import shape

import json
with open("states.geojson") as fl:
    states = json.load(fl)

for feature in states['features']:
    if feature['properties']['STATE_NAME'] == 'New South Wales':
        nsw = shape(feature)

with open("ga_summary_grid_landsat_c3.geojson") as fl:
    au_grid = json.load(fl)

output = {
    'type': au_grid['type'],
    'features': []
}

for feature in au_grid['features']:
    tile = shape(feature)
    if tile.intersects(nsw):
        output['features'].append(feature)

with open("nsw_tiles.geojson", "w") as fl:
    json.dump(output, fl, indent=4)
