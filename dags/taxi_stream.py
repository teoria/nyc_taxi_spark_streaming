import os
import sys

#
# if os.path.exists("h3_pyspark.zip"):
#     sys.path.insert(0, 'h3_pyspark.zip')
# else:
#     sys.path.insert(0, '../venv/lib/python3.9/site-packages')


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    import h3_pyspark

    spark = (SparkSession.builder
             .master("local[*]")
             .appName("taxi_events")
             .getOrCreate()
             )

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.endpoint", "http://192.168.1.7:9000")
    hadoop_conf.set("fs.s3a.access.key", "minio")
    hadoop_conf.set("fs.s3a.secret.key", "minio123")
    hadoop_conf.set("fs.s3a.path.style.access", "True")

    print("Load nyc_taxi_zones_h3_10 data ")
    nyc_zones = spark.read.format("parquet") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("../data/nyc_taxi_zones_h3_10.parquet.gzip")

    nyc_zones.show(1)

    socketDF = (spark.readStream
                .format("socket")
                .option("host", "localhost")
                .option("port", 5557)
                .load()
                )

    df = socketDF

    delimiter = ","
    split_col = split(df['value'], ',')
    df = df.withColumn('key', split_col.getItem(0))
    df = df.withColumn('fare_amount', split_col.getItem(1).cast(FloatType()))
    df = df.withColumn('pickup_datetime', split_col.getItem(2).cast(TimestampType()))
    df = df.withColumn('pickup_longitude', split_col.getItem(3).cast(FloatType()))
    df = df.withColumn('pickup_latitude', split_col.getItem(4).cast(FloatType()))
    df = df.withColumn('dropoff_longitude', split_col.getItem(5).cast(FloatType()))
    df = df.withColumn('dropoff_latitude', split_col.getItem(6).cast(FloatType()))
    df = df.withColumn('passenger_count', split_col.getItem(7).cast(IntegerType()))
    df = df.withColumn('event_date', to_date(col("pickup_datetime"), 'yyyy-MM-dd HH:mm:ss'))
    df = df.withColumn('year', year(col("event_date")))
    df = df.withColumn('month', month(col("event_date")))
    df = df.withColumn('day', dayofmonth(col("event_date")))
    df = df.withColumn("ingestion_date", current_date())
    df = df.withColumn('resolution', lit(10))
    df = df.withColumn('pickup_h3_10', h3_pyspark.geo_to_h3('pickup_latitude', 'pickup_longitude', 'resolution'))
    df = df.withColumn('dropoff_h3_10', h3_pyspark.geo_to_h3('dropoff_latitude', 'dropoff_longitude', 'resolution'))
    df_not_zero = df.filter(df.pickup_longitude != 0) \
        .drop("value") \
        .drop('resolution')

    df_join = df_not_zero.join(nyc_zones, (df_not_zero['pickup_h3_10'] == nyc_zones['h3_polyfill']))

    df_select = df_join \
        .select(col('key'),
                col('ingestion_date'),
                col('event_date'),
                col('year'),
                col('month'),
                col('day'),
                col('pickup_datetime'),
                col('zone'),
                col('borough'),
                col('pickup_h3_10'),
                col('dropoff_h3_10'),
                col('pickup_longitude'),
                col('pickup_latitude'),
                col('dropoff_longitude'),
                col('dropoff_latitude'),
                col('passenger_count'),
                col('fare_amount'))

    query = (df_select.writeStream
             .format("parquet")
             .outputMode("append")
             .option("checkpointLocation", "./lake/stage/check")
             .option("path", "./lake/stage/data")
             .partitionBy('ingestion_date')
             .trigger(processingTime='60 seconds')
             .start()
             )

    query.awaitTermination()
