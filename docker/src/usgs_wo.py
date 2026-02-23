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

measurements = ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22']
masking_band = "qa_pixel"
output_bands = ['water', 'elevation']

s3_bucket = "imam-dev-bucket"
s3_prefix = "usgs-wo"

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
    masking_data.attrs['nodata'] = 1

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


def calculate_wofs(data_t, dsm):
    # there should be only one time slice anyway
    data = data_t.isel(time=0)

    spectal_bands = data[measurements].to_array(dim="band")

    wet_dry = classifier.classify(spectal_bands)
    fmask = fmask_filter(data['fmask'])
    eo = eo_filter(data)
    terrain = terrain_filter(dsm, data, dsm.elevation.attrs.get('nodata', numpy.nan))
    water = wet_dry | fmask | eo | terrain

    _fix_nodata_to_single_value(water)

    assert water.dtype == numpy.uint8
    water = water.expand_dims(dim={"time": data_t['time']}, axis=0)

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
    ds['fmask'] = (ds['qa_pixel'].dims, qa_to_fmask(ds['qa_pixel']))
    ds['fmask'].attrs['nodata'] = 0
    del ds['qa_pixel']

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
