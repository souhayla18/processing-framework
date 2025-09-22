# src/writer.py
import os
import shutil
import glob
from pyspark.sql import DataFrame

def write_data(df: DataFrame, path: str, format: str = "json", mode: str = "overwrite", single_file: bool = True, single_name: str = "output.json"):
    if df is None:
        return

    # write to a temp folder
    tmp = path.rstrip("/") + "_tmp"
    if os.path.exists(tmp):
        shutil.rmtree(tmp)

    if format == "json":
        df.coalesce(1).write.mode(mode).json(tmp)
    elif format == "parquet":
        df.coalesce(1).write.mode(mode).parquet(tmp)
    else:
        df.coalesce(1).write.mode(mode).json(tmp)

    # ensure final folder exists
    os.makedirs(path, exist_ok=True)

    # move the part file to the final destination
    parts = glob.glob(os.path.join(tmp, "part-*"))
    if parts:
        dest_file = os.path.join(path, single_name)
        shutil.move(parts[0], dest_file)  # <-- move instead of copy

    # remove tmp folder completely
    shutil.rmtree(tmp)

    # remove any leftover non-json files in the destination folder
    for f in os.listdir(path):
        if not f.endswith(".json"):
            os.remove(os.path.join(path, f))
