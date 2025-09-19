from pyspark.sql import SparkSession

def get_spark():
    """
    Creates and returns a SparkSession with basic tuning configs.

    - appName: sets a name for tracking/logging
    - spark.executor.memory: limits memory per executor (2 GB is fine for this job)
    - spark.sql.shuffle.partitions: controls number of shuffle tasks (reduced from default 200 to 4 for efficiency, Number of shuffle partitions should be close to number of CPU cores available for the job.)
    """
    return (
        SparkSession.builder
        .appName("OminimoDE")
        .config("spark.executor.memory", "2g")            
        .config("spark.sql.shuffle.partitions", 4)         
        .getOrCreate()
    )
