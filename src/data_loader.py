def load_data(spark, source):
    """
    Generic loader that reads based on metadata source descriptor.
    source: dict with keys name, path, format
    """
    fmt = source.get("format", "json").lower()
    path = source["path"]
    if fmt == "json":
        return spark.read.json(path)
    elif fmt == "parquet":
        return spark.read.parquet(path)
    elif fmt in ("csv", "text"):
        return spark.read.option("header", True).csv(path)
    else:
        raise ValueError(f"Unsupported format: {fmt}")
