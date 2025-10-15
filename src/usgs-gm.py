# run this with GASuperUser credentials
# also install zarr?
"Special thanks to Chad Barton"
import numpy
import xarray
from pystac_client import Client
from dask import array as da
import dask.distributed
from datetime import datetime

from odc.geo import BoundingBox
from odc.geo.xr import write_cog
from odc.stac import configure_rio, stac_load
from odc.algo import xr_geomedian

from odc.io.cgroups import get_cpu_quota


def rewrite_asset_urls(in_url):
    http_prefix = 'https://landsatlook.usgs.gov/data/'
    s3_prefix = 's3://usgs-landsat/'
    if not in_url.startswith(http_prefix):
        return in_url
    return s3_prefix + in_url[len(http_prefix):]

start_date = "2023-01-01"
end_date = "2023-12-31"

crs="EPSG:4326"
central_lat =  -35.0
central_lon =  150.0
buffer = 0.1
latitude = (central_lat - buffer, central_lat + buffer)
longitude = (central_lon - buffer, central_lon + buffer)

measurements = ['coastal', 'blue', 'green', 'red', 'nir08', 'swir16', 'swir22']
masking_band = "qa_pixel"

bbox = BoundingBox(
    left=central_lon - buffer,
    bottom=central_lat - buffer,
    right=central_lon + buffer,
    top=central_lat + buffer,
    crs=crs
)


def search(stac_client):
    l2col = 'landsat-c2l2-sr'
    collections_query = [l2col]

    date_query = f"{start_date}/{end_date}"

    return stac_client.search(
        collections=collections_query,
        datetime=date_query,
        bbox=bbox.bbox
    ).item_collection()


def load(items):
    output_crs = "EPSG:3577"

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

def write(ds):
    for i, time in enumerate(ds['time']):
        for band in measurements + [masking_band]:
            write_cog(ds[band].isel(time=i), f'/output/{band}_{i}.tif', overwrite=True)

    
    # ds.to_zarr("s3://imam-dev-bucket/test1/test.zarr")

def write_geomedian(gm):
    for band in measurements:
       write_cog(gm[band], f'/output/{band}_gm.tif', overwrite=True) #.compute()
    # gm.to_zarr("/output/gm.zarr", mode="w")


def main():
    dask_client = dask.distributed.Client(n_workers=1, threads_per_worker=get_cpu_quota())
    configure_rio(cloud_defaults=True, aws={"requester_pays": True}, client=dask_client)

    stac_client = Client.open("https://landsatlook.usgs.gov/stac-server")

    print('searching', datetime.now())
    items = search(stac_client)
    print('loading', datetime.now())
    ds = load(items).persist()
    print('geomedian', datetime.now())
    gm = xr_geomedian(ds).load()
    print('writing', datetime.now())
    write_geomedian(gm)
    print('done', datetime.now())

if __name__ == '__main__':
    main()
