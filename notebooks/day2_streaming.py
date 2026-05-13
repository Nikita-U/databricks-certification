# Databricks notebook source
# Simulate a live data stream using Databricks "rate" source
# This generates 2 rows per second automatically

stream_df = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 2)
    .load())

stream_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, when, current_timestamp

# Transform the stream in real time
transformed_stream = (stream_df
    .withColumn("transaction_id", col("value"))
    .withColumn("amount", (col("value") * 100) % 5000)
    .withColumn("category", 
        when(col("value") % 3 == 0, "salary")
        .when(col("value") % 3 == 1, "groceries")
        .otherwise("utilities"))
    .withColumn("processed_at", current_timestamp())
    .drop("value")
)

transformed_stream.printSchema()

# COMMAND ----------

# Step 1 — Create a volume to store checkpoint
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.checkpoints")

# Step 2 — Run the stream with proper checkpoint path
query = (transformed_stream.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/checkpoints/stream1")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("streaming_transactions")
)

query.awaitTermination()
print("✅ Stream completed!")

# Step 3 — Check results
spark.sql("SELECT * FROM streaming_transactions").show()

# COMMAND ----------

# Clear old checkpoint and run again
spark.sql("DROP TABLE IF EXISTS streaming_transactions")

query = (transformed_stream.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/checkpoints/stream2")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("streaming_transactions")
)

query.awaitTermination()

# Check how many rows we got
spark.sql("SELECT COUNT(*) as total_rows FROM streaming_transactions").show()
spark.sql("SELECT * FROM streaming_transactions ORDER BY transaction_id").show(20)

# COMMAND ----------

# Generate more rows by increasing rate and adding a delay
from pyspark.sql.functions import col, when, current_timestamp
import time

# Step 1 — Create a faster stream
stream_df = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 50)  # 50 rows per second
    .option("numPartitions", 2)
    .load())

# Step 2 — Transform
transformed_stream = (stream_df
    .withColumn("transaction_id", col("value"))
    .withColumn("amount", (col("value") * 100) % 5000)
    .withColumn("category",
        when(col("value") % 3 == 0, "salary")
        .when(col("value") % 3 == 1, "groceries")
        .otherwise("utilities"))
    .withColumn("processed_at", current_timestamp())
    .drop("value")
)

# Step 3 — Drop old table and checkpoint
spark.sql("DROP TABLE IF EXISTS streaming_transactions")

# Step 4 — Write with small wait
query = (transformed_stream.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/checkpoints/stream3")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("streaming_transactions")
)

query.awaitTermination()

# Step 5 — Check results
spark.sql("SELECT COUNT(*) as total_rows FROM streaming_transactions").show()
spark.sql("SELECT * FROM streaming_transactions ORDER BY transaction_id LIMIT 10").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     COUNT(*)              AS total_transactions,
# MAGIC     ROUND(AVG(amount), 2) AS avg_amount,
# MAGIC     MIN(amount)           AS min_amount,
# MAGIC     MAX(amount)           AS max_amount
# MAGIC FROM streaming_transactions
# MAGIC GROUP BY category
# MAGIC ORDER BY total_transactions DESC