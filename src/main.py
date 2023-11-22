import configparser
from ETL.transform import read_from_database
from driver.spark_session import create_spark_session

# Load configuration
config = configparser.ConfigParser()
config.read('config.ini')

spark = create_spark_session(config['spark'])

input_dfs_dict = read_from_database(spark, config['database'])

spark.stop()