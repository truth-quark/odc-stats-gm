"""Special thanks to Chad Barton for the PoC"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
import multiprocessing
from pathlib import Path
import random
import sys

import boto3
import botocore
import numpy
from pystac_client import Client
from pystac import ItemCollection

from odc.algo import xr_geomedian
from odc.geo import BoundingBox
from odc.geo.xr import write_cog, assign_crs
from odc.stac import configure_rio, stac_load


query_crs = "EPSG:4326"
output_crs = "EPSG:3577"   # we probably want the native CRS for Solomons

measurements = ['coastal', 'blue', 'green', 'red', 'nir08', 'swir16', 'swir22']
masking_band = "qa_pixel"

product = "HY"  # [HY, FY]
s3_bucket = "dea-dme-dev"
s3_prefix = "products/solomons/geomad"


def log(*args, **kwargs):
    print(*args, **kwargs)
    sys.stdout.flush()


def read_tasks_list():
    with open("/src/gm_tasks.list") as fl:
        return [line.strip() for line in fl]


def write_tasks_list(tasks_list):
    with open("/src/gm_tasks.list", "w") as fl:
        for task in tasks_list:
            print(task, file=fl)


def rewrite_asset_urls(in_url):
    http_prefix = 'https://landsatlook.usgs.gov/data/'
    s3_prefix = 's3://usgs-landsat/'
    if not in_url.startswith(http_prefix):
        return in_url
    return s3_prefix + in_url[len(http_prefix):]


def find_feature(region_code):
    with open("/src/gm_polygons.geojson") as fl:
        data = json.load(fl)

    features = data['features']

    for feature in features:
        if feature['properties']['region_code'] == region_code:
            return feature

    raise ValueError(f"region not found: {region_code}")


def bounds(feature):
    geom = feature['geometry']
    assert geom['type'] == "Polygon"
    coords = geom['coordinates']
    assert len(coords) == 1
    points = coords[0]
    lons = [p[0] for p in points]
    lats = [p[1] for p in points]
    left, right = min(lons), max(lons)
    top, bottom = max(lats), min(lats)

    return BoundingBox(
        left=left, top=top, right=right, bottom=bottom, crs=query_crs
    )


def search(bbox):
    stac_client = Client.open("https://landsatlook.usgs.gov/stac-server")
    l2col = 'landsat-c2l2-sr'

    start_date = f"2023-01-01"
    end_date = f"2023-12-31"
    date_query = f"{start_date}/{end_date}"

    landsat8 = stac_client.search(
        collections=[l2col],
        datetime=date_query,
        bbox=bbox.bbox,
        query={"platform": {"eq": "LANDSAT_8"}}
    ).item_collection().items

    landsat9 = stac_client.search(
        collections=[l2col],
        datetime=date_query,
        bbox=bbox.bbox,
        query={"platform": {"eq": "LANDSAT_9"}}
    ).item_collection().items

    return ItemCollection(sorted(landsat8 + landsat9, key=lambda item: item.properties['datetime']))


def load_mask(items, bbox):
    with ThreadPoolExecutor() as pool:
        mask_ds = stac_load(
            items=items,
            bands=[masking_band],
            crs=output_crs,
            resolution=30,
            bbox=bbox,
            resampling="nearest",
            dtype="int32",
            pool=pool,
            patch_url=rewrite_asset_urls,
        )

    masking_data = mask_ds[masking_band]
    return ((masking_data & 1) == 0) & ((masking_data & (1 << 6)) == (1 << 6))


def load_optical(items, bbox):
    with ThreadPoolExecutor() as pool:
        optical_ds = stac_load(
            items=items,
            bands=measurements,
            crs=output_crs,
            resolution=30,
            bbox=bbox,
            resampling="average",
            dtype="float32",
            pool=pool,
            patch_url=rewrite_asset_urls,
        )

    scale = 0.00002750
    offset = -0.200000
    rescale = 10000.0

    for band in measurements:
        optical_ds[band] = ((optical_ds[band] * scale + offset) * rescale)
    return optical_ds


def load(items, bbox):
    mask = load_mask(items, bbox)
    optical_ds = load_optical(items, bbox)

    for band in measurements:
        optical_ds[band] = (optical_ds[band].dims, numpy.where(mask, optical_ds[band], numpy.nan))

    return optical_ds


def write_input_data(ds):
    for i, time in enumerate(numpy.datetime_as_string(ds['time'].data)):
        for band in measurements:
            write_cog(ds[band].isel(time=i).compute(), f'/output/{band}_{time}_{i}.tif', overwrite=True)


def write_geomedian(gm, region_code, upload=True):
    if upload:
        s3_client = boto3.client('s3')
    else:
        s3_client = None

    root = Path("/output")
    folder = f"usgs_ls_gm/{region_code}"
    (root / folder).mkdir(parents=True, exist_ok=True)

    for band in measurements:
       filename = f'{folder}/gm_{product}_{region_code}_{band}.tif'
       on_disk = str(root / filename)
       write_cog(gm[band], on_disk, overwrite=True)
       if upload:
           s3_client.upload_file(on_disk, s3_bucket, f"{s3_prefix}/{filename}")

    filename = f"{folder}/gm_{product}_{region_code}.completed"
    on_disk = str(root / filename)
    with open(on_disk, "w") as fl:
        print("done!", file=fl)
    if upload:
        s3_client.upload_file(on_disk, s3_bucket, f"{s3_prefix}/{filename}")


def check_exists(region_code):
    s3_client = boto3.client('s3')
    folder = f"usgs_ls_gm/{region_code}"
    filename = f"{folder}/gm_{product}_{region_code}.completed"
    try:
        s3_client.head_object(Bucket=s3_bucket, Key=f"{s3_prefix}/{filename}")
        return True
    except botocore.exceptions.ClientError:
        return False


def execute_task(region_code):
    configure_rio(cloud_defaults=True, aws={"requester_pays": True})

    bbox = bounds(find_feature(region_code))
    log('searching', bbox.bbox, region_code, datetime.now())
    items = search(bbox)
    log('loading', datetime.now())
    ds = load(items, bbox)
    log('geomedian', datetime.now())
    gm = assign_crs(xr_geomedian(ds, num_threads=multiprocessing.cpu_count()), crs=output_crs)
    log('writing', datetime.now())
    write_geomedian(gm, region_code)

    log('done', datetime.now())


def main():
    tasks_list = read_tasks_list()

    while tasks_list != []:
        region_code = random.choice(tasks_list)

        if not check_exists(region_code):
            execute_task(region_code)
        else:
            log(region_code, 'already exists!')

        tasks_list.remove(region_code)
        write_tasks_list(tasks_list)

if __name__ == '__main__':
    main()
