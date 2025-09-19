def load_data(spark, source):
    return spark.read.json(source["path"])
