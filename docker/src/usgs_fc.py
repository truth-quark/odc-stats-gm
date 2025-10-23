"""Special thanks to Chad Barton for the PoC"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
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
from odc.geo.xr import write_cog, assign_crs
from odc.stac import configure_rio, stac_load


year = 2023

query_crs="EPSG:4326"
output_crs = "EPSG:3577"

measurements = ['green', 'red', 'nir08', 'swir16', 'swir22']

s3_bucket = "imam-dev-bucket"
s3_prefix = "usgs-fc"


def log(*args, **kwargs):
    print(*args, **kwargs)
    sys.stdout.flush()


def read_tasks_list():
    with open("/src/fc_tasks.list") as fl:
        return [line.strip() for line in fl]


def write_tasks_list(tasks_list):
    with open("/src/fc_tasks.list", "w") as fl:
        for task in tasks_list:
            print(task, file=fl)


def rewrite_asset_urls(in_url):
    http_prefix = 'https://landsatlook.usgs.gov/data/'
    s3_prefix = 's3://usgs-landsat/'
    if not in_url.startswith(http_prefix):
        return in_url
    return s3_prefix + in_url[len(http_prefix):]


def search(scene_id):
    # TODO
    stac_client = Client.open("https://landsatlook.usgs.gov/stac-server")
    l2col = 'landsat-c2l2-sr'

    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
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


def load(items, bbox):
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


def write_input_data(ds):
    for i, time in enumerate(numpy.datetime_as_string(ds['time'].data)):
        for band in measurements:
            write_cog(ds[band].isel(time=i).compute(), f'/output/{band}_{time}_{i}.tif', overwrite=True)


def write_fc(fc, scene_id, upload=True):
    if upload:
        s3_client = boto3.client('s3')
    else:
        s3_client = None

    root = Path("/output")
    folder = scene_id
    (root / folder).mkdir(parents=True, exist_ok=True)

    for band in measurements:
       filename = f'{folder}/fc_{scene_id}_{band}.tif'
       on_disk = str(root / filename)
       write_cog(fc[band], on_disk, overwrite=True)
       if upload:
           s3_client.upload_file(on_disk, s3_bucket, f"{s3_prefix}/{filename}")

    filename = f"{folder}/{scene_id}.completed"
    on_disk = str(root / filename)
    with open(on_disk, "w") as fl:
        print("done!", file=fl)
    if upload:
        s3_client.upload_file(on_disk, s3_bucket, f"{s3_prefix}/{filename}")


def check_exists(scene_id):
    s3_client = boto3.client('s3')
    folder = scene_id
    filename = f"{folder}/{scene_id}.completed"
    try:
        s3_client.head_object(Bucket=s3_bucket, Key=f"{s3_prefix}/{filename}")
        return True
    except botocore.exceptions.ClientError:
        return False


def xr_fc(ds):
    pass


def execute_task(scene_id):
    configure_rio(cloud_defaults=True, aws={"requester_pays": True})

    log('searching', scene_id, datetime.now())
    items = search(scene_id)
    log('loading', datetime.now())
    ds = load(items)
    log('fc', datetime.now())
    # TODO output_crs
    fc = assign_crs(xr_fc(ds), crs=output_crs)
    log('writing', datetime.now())
    write_fc(fc, scene_id)

    log('done', datetime.now())


def main():
    tasks_list = read_tasks_list()

    while tasks_list != []:
        scene_id = random.choice(tasks_list)

        if not check_exists(scene_id):
            execute_task(scene_id)
        else:
            log(scene_id, 'already exists!')

        tasks_list.remove(scene_id)
        write_tasks_list(tasks_list)


if __name__ == '__main__':
    main()
