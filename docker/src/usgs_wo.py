from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
import sys
import random

import boto3
import botocore
import numpy
from pystac_client import Client
import xarray

from dask import array as da
from odc.geo.xr import write_cog, assign_crs
from odc.stac import configure_rio, stac_load

from wofs import classifier
from wofs.wofls import _fix_nodata_to_single_value
from wofs.filters import eo_filter, fmask_filter, terrain_filter, c2_filter

measurements = ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22']
masking_band = "qa_pixel"

s3_bucket = "imam-dev-bucket"
s3_prefix = "usgs-wo/"

task_list_file = "/src/tas_wo.list"


def log(*args, **kwargs):
    print(*args, **kwargs)
    sys.stdout.flush()


def read_tasks_list():
    with open(task_list_file) as fl:
        return [line.strip() for line in fl]


def write_tasks_list(tasks_list):
    with open(task_list_file, "w") as fl:
        for task in tasks_list:
            print(task, file=fl)


def rewrite_asset_urls(in_url):
    http_prefix = 'https://landsatlook.usgs.gov/data/'
    s3_prefix = 's3://usgs-landsat/'
    if not in_url.startswith(http_prefix):
        return in_url
    return s3_prefix + in_url[len(http_prefix):]


def search(scene_id):
    stac_client = Client.open("https://landsatlook.usgs.gov/stac-server")
    l2col = 'landsat-c2l2-sr'

    return stac_client.search(
        collections=[l2col],
        query={"landsat:scene_id": {"eq": scene_id}}
    ).item_collection()



def load(items):
    with ThreadPoolExecutor() as pool:
        optical_ds = stac_load(
            items=items,
            bands=measurements,
            pool=pool,
            patch_url=rewrite_asset_urls,
        )

    with ThreadPoolExecutor() as pool:
        mask_ds = stac_load(
            items=items,
            bands=[masking_band],
            dtype="int32",
            pool=pool,
            patch_url=rewrite_asset_urls,
        )

    scale = 0.00002750
    offset = -0.200000
    rescale = 10000.0

    masking_data = mask_ds[masking_band]
    mask = ((masking_data & 1) == 0)

    for band in measurements:
        data = ((optical_ds[band] * scale + offset) * rescale)
        data = numpy.clip(data, 0, 10000)
        data = data.where((mask | ~numpy.isnan(data)))
        data = data.fillna(-999.0)
        data = data.astype('int16')
        optical_ds[band] = data
        optical_ds[band].attrs['nodata'] = -999

    return xarray.merge([optical_ds, mask_ds])


def write_input_data(scene_id, ds):
    Path(f'/output/{scene_id}').mkdir(parents=True, exist_ok=True)

    for i, time in enumerate(numpy.datetime_as_string(ds['time'].data)):
        for band in measurements + ['fmask', 'water']:
            write_cog(ds[band].isel(time=i).compute(), f'/output/{scene_id}/{band}_{time}_{i}.tif', overwrite=True)


def write_geomedian(gm, region_code, upload=True):
    if upload:
        s3_client = boto3.client('s3')
    else:
        s3_client = None

    root = Path("/output")
    folder = f"usgs_ls_gm/{region_code}"
    (root / folder).mkdir(parents=True, exist_ok=True)

    for band in measurements:
       filename = f'{folder}/gm_{region_code}_{band}_{year}.tif'
       on_disk = str(root / filename)
       write_cog(gm[band], on_disk, overwrite=True)
       if upload:
           s3_client.upload_file(on_disk, s3_bucket, f"{s3_prefix}/{filename}")

    filename = f"{folder}/{region_code}_{year}.completed"
    on_disk = str(root / filename)
    with open(on_disk, "w") as fl:
        print("done!", file=fl)
    if upload:
        s3_client.upload_file(on_disk, s3_bucket, f"{s3_prefix}/{filename}")


def check_exists(region_code):
    return False

    # TODO
    s3_client = boto3.client('s3')
    folder = f"usgs_ls_gm/{region_code}"
    filename = f"{folder}/{region_code}_{year}.completed"
    try:
        s3_client.head_object(Bucket=s3_bucket, Key=f"{s3_prefix}/{filename}")
        return True
    except botocore.exceptions.ClientError:
        return False


def qa_to_fmask(qa_pixel):
    # fill = qa_pixel & (1 << 0)
    dilated = qa_pixel & (1 << 1)
    cirrus = qa_pixel & (1 << 2)
    cloud = qa_pixel & (1 << 3)
    shadow = qa_pixel & (1 << 4)
    snow = qa_pixel & (1 << 5)
    clear = qa_pixel & (1 << 6)
    water = qa_pixel & (1 << 7)

    fmask = (qa_pixel * 0).astype('uint8')
    fmask = numpy.where(clear, 1, fmask)
    fmask = numpy.where(dilated | cirrus | cloud, 2, fmask)
    fmask = numpy.where(shadow, 3, fmask)
    fmask = numpy.where(snow, 4, fmask)
    fmask = numpy.where(water, 5, fmask)
    return fmask


def calculate_wofs(data_t):
    # there should be only one time slice anyway
    data = data_t.isel(time=0)

    dsm_path = 'https://dea-public-data.s3-ap-southeast-2.amazonaws.com/projects/elevation/ga_srtm_dem1sv1_0/dem1sv1_0.tif'
    terrain_buffer = 0

    spectal_bands = data[measurements].to_array(dim="band")

    # TODO terrain filter
    water = classifier.classify(spectal_bands) | fmask_filter(data['fmask']) | eo_filter(data)

    _fix_nodata_to_single_value(water)

    assert water.dtype == numpy.uint8
    water = water.expand_dims(dim={"time": data['time']}, axis=0)

    return water


def execute_task(scene_id):
    configure_rio(cloud_defaults=True, aws={"requester_pays": True})

    log('searching', scene_id, datetime.now())
    items = search(scene_id)
    log('loading', datetime.now())
    ds = load(items)
    ds['fmask'] = (ds['qa_pixel'].dims, qa_to_fmask(ds['qa_pixel']))
    ds['water'] = calculate_wofs(ds)
    log('writing', datetime.now())
    write_input_data(scene_id, ds)

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
