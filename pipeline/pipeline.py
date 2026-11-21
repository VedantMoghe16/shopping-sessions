#!/usr/bin/env python3
"""Run the native reducer and encode its typed batches as Parquet."""

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import struct
import subprocess
from tempfile import TemporaryDirectory
from time import monotonic
import sys

import pyarrow as pa
import pyarrow.parquet as pq


USER_SCHEMA = pa.schema([
    ("user_id", pa.string()),
    ("day", pa.date32()),
    ("sessions", pa.int64()),
    ("events", pa.int64()),
    ("revenue_cents", pa.int64()),
    ("top_category", pa.string()),
])
CAMPAIGN_SCHEMA = pa.schema([
    ("campaign_id", pa.string()),
    ("channel", pa.string()),
    ("day", pa.date32()),
    ("sessions", pa.int64()),
    ("purchasing_sessions", pa.int64()),
    ("revenue_cents", pa.int64()),
    ("assisted_sessions", pa.int64()),
])


def read_exact(file, size):
    data = file.read(size)
    if len(data) != size:
        raise ValueError("Incomplete native output batch")
    return data


def read_strings(file, count, nullable):
    valid = pa.py_buffer(read_exact(file, (count + 7) // 8)) if nullable else None
    offsets = read_exact(file, (count + 1) * 4)
    length = struct.unpack_from("<I", offsets, count * 4)[0]
    values = read_exact(file, length)
    return pa.Array.from_buffers(pa.string(), count, [valid, pa.py_buffer(offsets), pa.py_buffer(values)])


def write_part(source, destination, is_user):
    schema = USER_SCHEMA if is_user else CAMPAIGN_SCHEMA
    with source.open("rb") as file, pq.ParquetWriter(destination, schema, compression="snappy") as writer:
        while header := file.read(4):
            count, = struct.unpack("<I", header)
            first = read_strings(file, count, nullable=False)
            second = read_strings(file, count, nullable=True)
            day = pa.Array.from_buffers(pa.date32(), count, [None, pa.py_buffer(read_exact(file, count * 4))])
            numbers = [
                pa.Array.from_buffers(pa.int64(), count, [None, pa.py_buffer(read_exact(file, count * 8))])
                for _ in range(3 if is_user else 4)
            ]
            columns = [first, day, *numbers, second] if is_user else [first, second, day, *numbers]
            writer.write_batch(pa.RecordBatch.from_arrays(columns, schema=schema))


def main():
    data = Path(os.environ.get("DATA_DIR", "/data"))
    output = Path(os.environ.get("OUTPUT_DIR", "/output"))
    scratch = Path(os.environ.get("SCRATCH_DIR", "/scratch"))
    cpus = int(os.environ.get("NCPU", len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else 1))
    manifest = json.loads((data / "manifest.json").read_text(encoding="utf-8"))
    binary = os.environ.get("SHOPPING_NATIVE", "/app/shopping-native")
    users, campaigns = output / "user_daily", output / "campaign_daily"
    users.mkdir(parents=True, exist_ok=True)
    campaigns.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="shopping-", dir=scratch) as tmp:
        subprocess.run([binary, str(data), tmp, manifest["window_start"], manifest["window_end"], str(cpus)], check=True)
        before = monotonic()
        staging = Path(tmp)
        user_shards = [path for path in staging.glob("user-*") if path.stat().st_size]
        if not user_shards:
            user_shards = [staging / "user-0"]
        with ThreadPoolExecutor(max_workers=cpus) as pool:
            jobs = [
                pool.submit(write_part, source, users / f"part-{index}.parquet", True)
                for index, source in enumerate(user_shards)
            ]
            jobs.append(pool.submit(write_part, staging / "campaign", campaigns / "part-0.parquet", False))
            for job in jobs:
                job.result()
        print(f"parquet {monotonic() - before:.3f} s", file=sys.stderr)


if __name__ == "__main__":
    main()
