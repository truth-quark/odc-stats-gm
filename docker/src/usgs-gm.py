"""Special thanks to Chad Barton for the PoC"""

from datetime import datetime
import numpy
from pystac_client import Client
from dask import array as da
import dask.distributed

from odc.algo import xr_geomedian
from odc.geo import BoundingBox
from odc.geo.xr import write_cog, assign_crs
from odc.io.cgroups import get_cpu_quota
from odc.stac import configure_rio, stac_load


def rewrite_asset_urls(in_url):
    http_prefix = 'https://landsatlook.usgs.gov/data/'
    s3_prefix = 's3://usgs-landsat/'
    if not in_url.startswith(http_prefix):
        return in_url
    return s3_prefix + in_url[len(http_prefix):]

year = 2023

query_crs="EPSG:4326"
output_crs = "EPSG:3577"

measurements = ['coastal', 'blue', 'green', 'red', 'nir08', 'swir16', 'swir22']
masking_band = "qa_pixel"

central_lat =  -35.0
central_lon =  150.0
buffer = 0.1

bbox = BoundingBox(
    left=central_lon - buffer,
    bottom=central_lat - buffer,
    right=central_lon + buffer,
    top=central_lat + buffer,
    crs=query_crs
)


def search():
    stac_client = Client.open("https://landsatlook.usgs.gov/stac-server")
    l2col = 'landsat-c2l2-sr'
    collections_query = [l2col]

    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    date_query = f"{start_date}/{end_date}"

    return stac_client.search(
        collections=collections_query,
        datetime=date_query,
        bbox=bbox.bbox
    ).item_collection()


def load(items):
    optical_ds = stac_load(
        items=items,
        bands=measurements,
        crs=output_crs,
        resolution=30,
        groupby="solar_day",
        bbox=bbox,
        resampling="average",
        dtype="float32",
        chunks={"x": 600, "y": 600},
        patch_url=rewrite_asset_urls,
    )
    
    mask_ds = stac_load(
        items=items,
        bands=[masking_band],
        crs=output_crs,
        resolution=30,
        groupby="solar_day",
        bbox=bbox,
        resampling="nearest",
        dtype="int32",
        chunks={"x": 100, "y": 100},
        patch_url=rewrite_asset_urls,
    )

    scale = 0.00002750
    offset = -0.200000
    rescale = 10000.0

    masking_data = mask_ds[masking_band]
    mask = ((masking_data & 1) == 0) & ((masking_data & (1 << 6)) == (1 << 6))

    for band in measurements:
        optical_ds[band] = (optical_ds[band] * scale + offset) * rescale
        where = da.where(mask, optical_ds[band], numpy.nan)
        optical_ds[band] = (optical_ds[band].dims, where)

    return optical_ds

def write_input_data(ds):
    for i, _ in enumerate(ds['time']):
        for band in measurements + [masking_band]:
            write_cog(ds[band].isel(time=i), f'/output/{band}_{i}.tif', overwrite=True)

def write_geomedian(gm):
    for band in measurements:
       write_cog(gm[band], f'/output/{band}_gm_{year}.tif', overwrite=True)

def setup():
    dask_client = dask.distributed.Client(n_workers=1, threads_per_worker=get_cpu_quota())
    configure_rio(cloud_defaults=True, aws={"requester_pays": True}, client=dask_client)

def main():
    setup()

    print('searching', datetime.now())
    items = search()
    print('loading', datetime.now())
    ds = load(items).persist()
    print('geomedian', datetime.now())
    gm = assign_crs(xr_geomedian(ds).load(), crs=output_crs)
    print('writing', datetime.now())
    write_geomedian(gm)
    print('done', datetime.now())

if __name__ == '__main__':
    main()
