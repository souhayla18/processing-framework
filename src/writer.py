# src/writer.py
import os
import shutil
import glob
from pyspark.sql import DataFrame

def write_data(df: DataFrame, path: str, format: str = "json", mode: str = "overwrite", single_file: bool = False, single_name: str = "output.json"):
    if df is None:
        return

    if single_file:
        # write to a temp subfolder then merge part file to single file
        tmp = path.rstrip("/") + "_tmp"
        if os.path.exists(tmp):
            shutil.rmtree(tmp)
        if format == "json":
            df.coalesce(1).write.mode(mode).json(tmp)
        elif format == "parquet":
            df.coalesce(1).write.mode(mode).parquet(tmp)
        else:
            df.coalesce(1).write.mode(mode).json(tmp)

        # find the part file and move it as single_name into final path
        os.makedirs(path, exist_ok=True)
        part_pattern = os.path.join(tmp, "part-*")
        parts = glob.glob(part_pattern)
        if not parts:
            # fallback: copy whole folder
            shutil.copytree(tmp, path, dirs_exist_ok=True)
        else:
            dest_file = os.path.join(path, single_name)
            shutil.copyfile(parts[0], dest_file)

        # remove tmp
        shutil.rmtree(tmp)
    else:
        if format == "json":
            df.write.mode(mode).json(path)
        elif format == "parquet":
            df.write.mode(mode).parquet(path)
        else:
            df.write.mode(mode).json(path)
