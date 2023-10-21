import pyspark
from pyspark.sql import  SparkSession

from pyspark.sql.functions import *
# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()

spark.sql("set spark.sql.streaming.schemaInference=true")
# Monitor the logs directory for new log data, and read in the raw lines 

data = spark.readStream.format("csv") \
    .option("header", "true") \
    .load("logs")
data = data.withColumn("input_timestamp", to_timestamp("timestamp"))
# Set the window duration to 10 minutes
df = data.withColumn(
    "window", pyspark.sql.functions.window(data["input_timestamp"], "2 seconds"))

# Calculate the average price of the stock in each window
df = df.groupBy("window").agg(pyspark.sql.functions.avg("temp"))


# Start the streaming query
query = df.writeStream.outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()


# Wait for the streaming query to finish
query.awaitTermination()

# Cleanly shut down the session
spark.stop()

