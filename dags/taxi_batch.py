from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os
from datetime import datetime, timedelta

if __name__ == '__main__':
    os.environ["HADOOP_USER_NAME"] = "hdfs"
    spark = (SparkSession.builder
                .master("local[1]")
                .appName("batch")
                .getOrCreate()
             )
    spark.sparkContext.setLogLevel("ERROR")

    base_date = datetime.strptime(sys.argv[1], '%Y-%m-%d').date() - timedelta(days=1)
    path = f"./lake/raw/data/ingestion_date={base_date}"
    print(f'Read path: {path}')

    try:
        df_raw_daily = spark.read.parquet(path)
    except AnalysisException as e:
        print(e)
        exit(1)

    df_raw_daily = df_raw_daily\
        .withColumn("revision_date", current_timestamp())\
        .withColumn("ingestion_date", lit(base_date))\
        .select(
            'key',
            'event_date',
            'ingestion_date',
            'pickup_datetime',
            'zone', 'borough',
            'pickup_h3_10',
            'dropoff_h3_10',
            'pickup_longitude',
            'pickup_latitude',
            'dropoff_longitude',
            'dropoff_latitude',
            'passenger_count',
            'fare_amount',
            'revision_date',
            'year', 'month', 'day'
        )
    df_raw_daily.show(5)

    try:
        df_old = spark.read.parquet("./lake/curated/")
        df_new = df_old.union(df_raw_daily).dropDuplicates(['key'])
    except AnalysisException as e:
        df_new = df_raw_daily

    df_new.write.mode("overwrite")\
        .partitionBy('year', 'month', 'day')\
        .format("parquet")\
        .save("./lake/curated/")

    print("Done")
