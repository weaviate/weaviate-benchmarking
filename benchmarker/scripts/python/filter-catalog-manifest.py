#!/usr/bin/env python3
"""Write the filter-catalog manifest consumed by `benchmarker ann-benchmark --filterCatalog`.

    filter-catalog-manifest.py <dataset.hdf5> <manifest.json>

Expects filters/{global,per_query}/<name> packed bitmaps and
ground_truth/{global,per_query}/{names,neighbors}. Per-query filter names must
look like anticorr_q<row>_tau<tau>; <row> is the test query the filter belongs to.
"""
import json
import re
import sys

import h5py
import numpy as np


def decode(names):
    return [n.decode() if isinstance(n, bytes) else str(n) for n in names]


def main(src, dst):
    f = h5py.File(src, "r")
    out = []
    for group in ("global", "per_query"):
        names = decode(f[f"ground_truth/{group}/names"][:])
        for name in f[f"filters/{group}"]:
            d = f[f"filters/{group}"][name]
            size = int(d.attrs["size"]) if "size" in d.attrs else int(np.unpackbits(d[:]).sum())
            entry = {"name": name, "group": group, "gt_index": names.index(name), "query_row": -1,
                     "size": size, "family": str(d.attrs.get("family", "")), "tau": "",
                     "params": str(d.attrs.get("params", ""))}
            if group == "per_query":
                m = re.fullmatch(r"anticorr_q(\d+)_tau([\d.]+)", name)
                if not m:
                    sys.exit(f"cannot parse per_query filter name {name!r}")
                entry["query_row"] = int(m.group(1))
                entry["tau"] = m.group(2)
                entry["family"] = "anticorr"
            out.append(entry)
    json.dump(out, open(dst, "w"), indent=1)
    print(f"{len(out)} filters -> {dst}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
