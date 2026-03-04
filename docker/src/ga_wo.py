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

from odc.geo.xr import write_cog
from odc.stac import configure_rio, stac_load
from datacube.testutils.io import dc_read


from wofs import classifier
from wofs.wofls import _fix_nodata_to_single_value
from wofs.filters import eo_filter, fmask_filter, terrain_filter

measurements = ['nbart_blue', 'nbart_green', 'nbart_red', 'nbart_nir', 'nbart_swir_1', 'nbart_swir_2']
masking_band = "oa_fmask"
output_bands = ['water', 'elevation']

s3_bucket = "imam-dev-bucket"
s3_prefix = "ga-wo"

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


def region_code(scene_id):
    return scene_id[3:9]


def rewrite_asset_urls(in_url):
    http_prefix = 'https://landsatlook.usgs.gov/data/'
    s3_prefix = 's3://usgs-landsat/'
    if not in_url.startswith(http_prefix):
        return in_url
    return s3_prefix + in_url[len(http_prefix):]


def search(scene_id):
    stac_client = Client.open("https://explorer.dea.ga.gov.au/stac")

    return stac_client.search(
        collections=["ga_ls8c_ard_3", "ga_ls9c_ard_3"],
        filter={"op": "eq", "args": [{"property": "landsat:landsat_scene_id"}, scene_id]}
    ).item_collection()



def load(items):
    with ThreadPoolExecutor() as pool:
        optical_ds = stac_load(
            items=items,
            bands=measurements,
            pool=pool,
        )

    with ThreadPoolExecutor() as pool:
        mask_ds = stac_load(
            items=items,
            bands=[masking_band],
            dtype="uint8",
            pool=pool,
        )

    for band in measurements:
        data = optical_ds[band]
        data = data.fillna(-999.0)
        data = data.astype('int16')
        optical_ds[band] = data
        optical_ds[band].attrs['nodata'] = -999

    mask_ds[masking_band].attrs['nodata'] = 0

    return xarray.merge([optical_ds, mask_ds])


def write_wo(scene_id, ds, upload=True):
    if upload:
        s3_client = boto3.client('s3')
    else:
        s3_client = None

    root = Path("/output")
    folder = f"{s3_prefix}/{region_code(scene_id)}"
    (root / folder).mkdir(parents=True, exist_ok=True)

    for band in output_bands:
       filename = f'{folder}/wo_{scene_id}_{band}.tif'
       on_disk = str(root / filename)
       write_cog(ds[band].isel(time=0), on_disk, overwrite=True)
       if upload:
           s3_client.upload_file(on_disk, s3_bucket, f"{filename}")

    filename = f"{folder}/wo_{scene_id}.completed"
    on_disk = str(root / filename)
    with open(on_disk, "w") as fl:
        print("done!", file=fl)
    if upload:
        s3_client.upload_file(on_disk, s3_bucket, f"{filename}")


def check_exists(scene_id):
    s3_client = boto3.client('s3')
    folder = f"{s3_prefix}/{region_code(scene_id)}"
    filename = f"{folder}/wo_{scene_id}.completed"
    try:
        s3_client.head_object(Bucket=s3_bucket, Key=f"{filename}")
        return True
    except botocore.exceptions.ClientError:
        return False


def calculate_wofs(data_t, dsm):
    # there should be only one time slice anyway
    data = data_t.isel(time=0)

    spectal_bands = data[measurements].to_array(dim="band")

    wet_dry = classifier.classify(spectal_bands)
    fmask = fmask_filter(data['oa_fmask'])
    eo = eo_filter(data)
    terrain = terrain_filter(dsm, data, dsm.elevation.attrs.get('nodata', numpy.nan))
    water = wet_dry | fmask | eo | terrain

    _fix_nodata_to_single_value(water)

    assert water.dtype == numpy.uint8
    water = water.expand_dims(dim={"time": data_t['time']}, axis=0)
    water.attrs['nodata'] = 1

    return water


def load_dsm(data):
    dsm_path = 's3://dea-non-public-data/dsm/dsm1sv1_0_Clean.tiff'
    terrain_buffer = 0

    gbox = data.odc.geobox.buffered(terrain_buffer, terrain_buffer)
    dsm = dc_read(dsm_path, geobox=gbox, resampling="bilinear")
    return xarray.Dataset(
            data_vars={'elevation': (('y', 'x'), dsm)},
            coords={dim: coord.values for dim, coord in gbox.coordinates.items()},
            attrs={'crs': gbox.crs}
        )


def execute_task(scene_id):
    configure_rio(cloud_defaults=True, aws={"requester_pays": True})

    log('searching', scene_id, datetime.now())
    items = search(scene_id)
    log('loading', datetime.now())
    ds = load(items)
    dsm = load_dsm(ds)

    log('calculating', datetime.now())

    ds['blue'] = ds['nbart_blue']
    ds['water'] = calculate_wofs(ds, dsm)
    ds['elevation'] = dsm['elevation'].expand_dims(dim={'time': ds['time']}, axis=0)

    log('writing', datetime.now())
    write_wo(scene_id, ds)

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
