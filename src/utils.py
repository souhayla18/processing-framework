from pyspark.sql import SparkSession

def get_spark(app_name="OminimoDE"):
    """
    Create a SparkSession with a few safe defaults for small-scale dev runs.
    Tweak or extend when running on a cluster.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", 4)
        .config("spark.python.worker.faulthandler.enabled", "true")
        .getOrCreate()
    )
