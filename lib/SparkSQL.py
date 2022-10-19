from pyspark.sql import *
from lib.logger import Log4j
from pyspark import SparkContext, SparkConf
if __name__=="__main__":
    conf = SparkConf() \
        .setMaster("local[3]") \
        .setAppName("SparkSQL")

    # sc = SparkContext(conf=conf)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # spark=SparkSession \
    # .builder \
    # .master("local") \
    # .appName("SparkSql") \
    # .enableHiveSupport() \
    # .getOrCreate()

    logger=Log4j(spark)

    # flightTime_df = spark.read.parquet("datasource/")
    # OR either read.parquet(<path>) ORR read.format(<formatofFile>).load(<Path>)
    path1="datasource/flight_time.parquet"
    print(path1)
    flightTime_df=spark.read.format("parquet").load(path1)

    # this is to create a new db named AIRLINE_DB under which the table will be saved
    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    flightTime_df.write.mode("overwrite").partitionBy("ORIGIN","OP_CARRIER").saveAsTable("MyParquetTable")

    logger.info(spark.catalog.listTables("AIRLINE_DB"))

    spark.stop()