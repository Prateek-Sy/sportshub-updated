from pyspark.sql import SparkSession

def create_spark_session(app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "postgresql-42.6.0.jar") \
        .config("spark.sql.broadcastTimeout", "200000") \
        .config("spark.driver.maxResultSize", "21g") \
        .config("spark.akka.frameSize", "2011") \
        .config("spark.yarn.executor.memoryOverhead", 2048) \
        .getOrCreate()
    
    return spark
