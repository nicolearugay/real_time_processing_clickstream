from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import logging

# Initialize logging
logging.basicConfig(filename='application.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaClickstreamConsumer") \
    .getOrCreate()

logging.info("Spark Session initialized.")

# Define the schema based on the JSON structure
clickstream_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("session_id", IntegerType(), True),
    StructField("timestamp", IntegerType(), True),
    StructField("page", StringType(), True),
    StructField("action", StringType(), True)
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "clickstream") \
    .load()

from pyspark.sql import functions as F
json_df = df.select(F.from_json(F.col("value").cast("string"), clickstream_schema).alias("parsed_value"))

# Perform transformation
transformed_df = json_df.select(
    F.col("parsed_value.user_id"),
    F.col("parsed_value.session_id"),
    F.col("parsed_value.timestamp"),
    F.col("parsed_value.page"),
    F.col("parsed_value.action")
)

logging.info("Data transformed.")

# Define function to process each batch
def process_batch(df, epoch_id):
    try:
        # Raw Data
        # Set up Snowflake credentials
        snowflake_options_raw = {
            "sfURL": "",
            "sfDatabase": "",
            "sfWarehouse": "",
            "sfRole": "",
            "sfSchema": "",
            "sfUser": "",
            "sfPassword": ""
        }

        df.write \
            .format("snowflake") \
            .options(**snowflake_options_raw) \
            .option("dbtable", "raw_clickstream_data") \
            .mode("append") \
            .save()

        # Enriched Data
        enriched_data = df.withColumn(
            "action_type",
            F.when(F.col("action") == "click", "CTA").otherwise("NAV")
        )
        # Set up Snowflake credentials 
        snowflake_options_enriched = {
            "sfURL": "",
            "sfDatabase": "",
            "sfWarehouse": "",
            "sfRole": "",
            "sfSchema": "",
            "sfUser": "",
            "sfPassword": ""
        }

        enriched_data.write \
            .format("snowflake") \
            .options(**snowflake_options_enriched) \
            .option("dbtable", "enriched_clickstream_data") \
            .mode("append") \
            .save()
    except Exception as e:
        logging.error(f"Failed to process batch {epoch_id}. Error: {e}")

# Use foreachBatch to call the function
query = transformed_df \
    .writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()

logging.info("Query terminated.")