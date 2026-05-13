# Databricks notebook source
i# Task 1 — Check Spark version
print(spark.version)

# Task 2 — Create your first DataFrame
data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])
df.show()

# Task 3 — Basic SQL
spark.sql("SELECT current_date(), current_timestamp()").show()

# Task 4 — Check current database
spark.sql("SELECT current_catalog(), current_database()").show()

# COMMAND ----------

# Create raw finance transactions data
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, to_date

data = [
    (1, "Alice",   "credit",  1500.00, "2026-01-01", "groceries"),
    (2, "Bob",     "debit",   200.00,  "2026-01-02", "utilities"),
    (3, "Alice",   "credit",  3000.00, "2026-01-03", "salary"),
    (4, "Charlie", "debit",   450.00,  "2026-01-04", "rent"),
    (5, "Bob",     "credit",  2500.00, "2026-01-05", "salary"),
    (6, "Alice",   "debit",   None,    "2026-01-06", "shopping"),   # bad record
    (7, "Charlie", "credit",  5000.00, "2026-01-07", "salary"),
    (8, "Bob",     "debit",   150.00,  "2026-01-08", "groceries"),
    (9, "Alice",   "credit",  1200.00, "2026-01-09", "freelance"),
    (10, "Charlie","debit",   300.00,  "2026-01-10", "utilities"),
]

schema = StructType([
    StructField("id",          IntegerType(), True),
    StructField("customer",    StringType(),  True),
    StructField("type",        StringType(),  True),
    StructField("amount",      DoubleType(),  True),
    StructField("date",        StringType(),  True),
    StructField("category",    StringType(),  True),
])

df = spark.createDataFrame(data, schema)
df.show()
print(f"Total records: {df.count()}")

# COMMAND ----------

# BRONZE — Save raw data as Delta table (no transformation, just store it)
df.write.format("delta").mode("overwrite").saveAsTable("bronze_transactions")

print("✅ Bronze layer created!")
spark.sql("SELECT COUNT(*) as total_records FROM bronze_transactions").show()
spark.sql("DESCRIBE TABLE bronze_transactions").show()

# COMMAND ----------

spark.sql("DESCRIBE HISTORY bronze_transactions").show(truncate=False)

# COMMAND ----------

# SILVER — Clean and validate Bronze data
from pyspark.sql.functions import col, to_date, upper, when

# Read from Bronze
bronze_df = spark.read.table("bronze_transactions")

# Transformation 1 — Remove NULL amounts (bad records)
# Transformation 2 — Convert date string to proper date type
# Transformation 3 — Standardise customer names to uppercase
# Transformation 4 — Add a new column for transaction flag

silver_df = (bronze_df
    .filter(col("amount").isNotNull())                        # remove NULLs
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))   # proper date type
    .withColumn("customer", upper(col("customer")))           # uppercase names
    .withColumn("transaction_flag",                           # flag high value
        when(col("amount") >= 2000, "HIGH")
        .when(col("amount") >= 500, "MEDIUM")
        .otherwise("LOW"))
)

silver_df.show()
print(f"Bronze records: {bronze_df.count()}")
print(f"Silver records: {silver_df.count()}")
print(f"Removed: {bronze_df.count() - silver_df.count()} bad records")

# COMMAND ----------

# Save Silver layer as Delta table
silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_transactions")

print("✅ Silver layer saved!")
spark.sql("DESCRIBE HISTORY silver_transactions").show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC select * from silver_transactions;
# MAGIC
# MAGIC -- GOLD — Business metrics
# MAGIC SELECT 
# MAGIC     customer,
# MAGIC     ROUND(SUM(amount), 2)   AS total_amount,
# MAGIC     ROUND(AVG(amount), 2)   AS avg_amount,
# MAGIC     COUNT(id)               AS total_transactions,
# MAGIC     ROUND(SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END), 2) AS total_credits,
# MAGIC     ROUND(SUM(CASE WHEN type = 'debit'  THEN amount ELSE 0 END), 2) AS total_debits
# MAGIC FROM silver_transactions
# MAGIC GROUP BY customer
# MAGIC ORDER BY total_amount DESC;
# MAGIC
# MAGIC -- Save Gold layer as Delta table
# MAGIC CREATE OR REPLACE TABLE gold_customer_summary AS
# MAGIC SELECT 
# MAGIC     customer,
# MAGIC     ROUND(SUM(amount), 2)   AS total_amount,
# MAGIC     ROUND(AVG(amount), 2)   AS avg_amount,
# MAGIC     COUNT(id)               AS total_transactions,
# MAGIC     ROUND(SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END), 2) AS total_credits,
# MAGIC     ROUND(SUM(CASE WHEN type = 'debit'  THEN amount ELSE 0 END), 2) AS total_debits
# MAGIC FROM silver_transactions
# MAGIC GROUP BY customer
# MAGIC ORDER BY total_amount DESC;
# MAGIC
# MAGIC SHOW TABLES;

# COMMAND ----------

display(
    spark.sql("DESCRIBE HISTORY bronze_transactions")
    .select("version", "timestamp", "operation", "operationParameters")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert matching the original bronze schema
# MAGIC INSERT INTO bronze_transactions 
# MAGIC     (id, customer, type, amount, date, category)
# MAGIC VALUES 
# MAGIC     (11, 'David', 'credit', 8000.00, '2026-01-11', 'salary')
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update a record (creates version 2)
# MAGIC UPDATE bronze_transactions 
# MAGIC SET amount = 9000.00 
# MAGIC WHERE id = 11

# COMMAND ----------

display(
    spark.sql("DESCRIBE HISTORY bronze_transactions")
    .select("version", "timestamp", "operation")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Current data (version 2)
# MAGIC SELECT * FROM bronze_transactions WHERE id = 11;
# MAGIC
# MAGIC -- Travel back to Version 0 (before David existed!)
# MAGIC SELECT * FROM bronze_transactions VERSION AS OF 0;
# MAGIC
# MAGIC -- Travel back to Version 1 (David added but not updated yet)
# MAGIC SELECT * FROM bronze_transactions VERSION AS OF 1
# MAGIC WHERE id = 11
# MAGIC
# MAGIC

# COMMAND ----------

print("=== Version 2 (current) ===")
display(spark.sql("SELECT * FROM bronze_transactions WHERE id = 11"))

print("=== Version 0 (before David) ===")
display(spark.sql("SELECT * FROM bronze_transactions VERSION AS OF 0"))

print("=== Version 1 (David before update) ===")
display(spark.sql("SELECT * FROM bronze_transactions VERSION AS OF 1 WHERE id = 11"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC DESCRIBE detail bronze_transactions;

# COMMAND ----------



bOPTIMIZE bronze_transactions


# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE bronze_transactions ZORDER BY (customer)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM bronze_transactions RETAIN 168 HOURS DRY RUN;
# MAGIC
# MAGIC display(spark.sql("select * from bronze_transactions"))