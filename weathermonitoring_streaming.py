import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType


def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .master("local[*]") \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .getOrCreate()
        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception: {e}")
    
    return s_conn

def weathermonitoring_streaming(spark_conn):
    print("Weather Monitoring Streaming with Kafka Demo Started ....")
    weather_detail_df = spark_conn.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "weather") \
        .option("startingOffsets", "latest") \
        .load()
    return weather_detail_df

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("CityName", StringType(), False),
        StructField("Temperature", DoubleType(), False),
        StructField("Humidity", IntegerType(), False),
        StructField("CreationTime", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    
    sel = sel.withColumn("CreationDate", col("CreationTime").cast(DateType()))
    weather_detail = sel.select("CityName", "Temperature", "Humidity", "CreationTime", "CreationDate")
    return weather_detail

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Connect to kafka with spark connection
        spark_df = weathermonitoring_streaming(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        print(selection_df)
        logging.info("Starting the streaming process...")

        try:
            weather_detail_write_stream = selection_df.writeStream \
                .trigger(processingTime="10 seconds") \
                .outputMode("append") \
                .format("console") \
                .start()
            # Write final result in HDFS
            selection_df.writeStream \
                .format("csv") \
                .option("path", "hdfs://localhost:9000/data/weather_detail")\
                .option("checkpointLocation", "hdfs://localhost:9000/data/weather_detail_checkpoint")\
                .outputMode("append") \
                .start()

            weather_detail_write_stream.awaitTermination()
        except Exception as e:
            logging.error(f"Streaming query failed due to exception: {e}")
        finally:
            spark_conn.stop()  
