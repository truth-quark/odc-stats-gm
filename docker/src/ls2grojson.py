"""
Quick & dirty tool to extract GeoJSON geometry content from a local copy of:
https://gist.githubusercontent.com/brunosan/9800909/raw/fe586ae06700bc2ab31eb423a8c7d65738fc6955/wrs2_descending.geojson

Given the path & 1+ Landsat path/row codes, this extracts the relevant feature
for pasting into the feature list to process.
"""

import os
import sys
import copy
import json

USAGE = "USAGE: ls2geojson <wrs2-descending-path-file> region-codes ..."


def extract_feature(region_code, path, row, features):
    # work on simplistic version first, loops over & over features
    for f in features:
        props = f["properties"]

        if props["PATH"] == path and props["ROW"] == row:
            return copy.copy(f)  # allow changes to data structure without affecting source

    msg = f"Could not find {path} {row}"
    raise RuntimeError(msg)


def update_properties(feature, region_code):
    """Remove unwanted keys from GitHub data & replace with region code."""
    feature["properties"] = {"region_code": region_code}


def region_code_to_path_row(raw_region_code):
    assert len(raw_region_code) == 6
    raw_codes = raw_region_code[:3], raw_region_code[3:]
    codes = [s.lstrip("0") for s in raw_codes]
    path, row = [int(n) for n in codes]
    return path, row


def main(wrs2, region_codes):
    features = wrs2["features"]
    results = []

    for region_code in region_codes:
        path, row = region_code_to_path_row(region_code)
        feature_dict = extract_feature(region_code, path, row, features)
        region_code_split = f"{region_code[:3]}_{region_code[3:]}"
        update_properties(feature_dict, region_code_split)
        results.append(json.dumps(feature_dict, indent=4))

    print(",\n".join(results))


if __name__ == "__main__":
    wrs2_path = sys.argv[1]
    raw_region_codes = sys.argv[2:]

    assert os.path.exists(wrs2_path), USAGE
    assert raw_region_codes, USAGE

    with open(wrs2_path) as wf:
        wrs2_geojson = json.load(wf)

    main(wrs2_geojson, raw_region_codes)
