import pyspark.sql.functions as F

# Create silver schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# Read bronze data
df_bronze = spark.read.table("bronze.sales_raw")

# ── Process clicked_items ──────────────────────────────────────────────────
df_silver_clicks = (
    df_bronze
    .select(
        "order_number",
        "customer_id",
        "customer_name",
        F.when(F.col("order_datetime") != "", F.to_timestamp(F.from_unixtime(F.col("order_datetime")))).otherwise(None).alias("order_datetime"),
        F.explode("clicked_items").alias("clicked_item")
    )
    .withColumn("clicked_product_id", F.col("clicked_item")[0])
    .withColumn("clicked_position", F.col("clicked_item")[1].cast("int"))
    .drop("clicked_item")
)

# Write to silver.clicks_clean
(df_silver_clicks
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.clicks_clean"))

# ── Process promo_info ─────────────────────────────────────────────────────
df_silver_promos = (
    df_bronze
    .select(
        "order_number",
        "customer_id",
        "customer_name",
        F.when(F.col("order_datetime") != "", F.to_timestamp(F.from_unixtime(F.col("order_datetime")))).otherwise(None).alias("order_datetime"),
        F.explode("promo_info").alias("promo")
    )
    .select(
        "order_number",
        "customer_id",
        "customer_name",
        "order_datetime",
        F.col("promo.promo_id").alias("promo_id"),
        F.col("promo.promo_disc").alias("promo_disc"),
        F.col("promo.promo_item").alias("promo_item"),
        F.col("promo.promo_qty").alias("promo_qty")
    )
)

# Write to silver.promos_clean
(df_silver_promos
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.promos_clean"))

# ── Process ordered_products ───────────────────────────
df_silver_sales = (
    df_bronze
    .select(
        "order_number",
        "customer_id",
        "customer_name",
        F.when(F.col("order_datetime") != "", F.to_timestamp(F.from_unixtime(F.col("order_datetime")))).otherwise(None).alias("order_datetime"),
        F.explode("ordered_products").alias("product")
    )
    .select(
        "order_number",
        "customer_id",
        "customer_name",
        "order_datetime",
        F.col("product.id").alias("product_id"),
        F.col("product.name").alias("product_name"),
        F.col("product.price").alias("price"),
        F.col("product.qty").alias("qty"),
        F.col("product.curr").alias("currency"),
        F.col("product.unit").alias("unit")
    )
)

# Write to silver.sales_clean
(df_silver_sales
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.sales_clean"))

# ── Verify all three tables ────────────────────────────────────────────────
print("=" * 80)
print("CLICKS_CLEAN TABLE")
print("=" * 80)
df_check_clicks = spark.read.table("silver.clicks_clean")
df_check_clicks.printSchema()
print(f"Total rows: {df_check_clicks.count()}")
df_check_clicks.show(10, truncate=False)

print("\n" + "=" * 80)
print("PROMOS_CLEAN TABLE")
print("=" * 80)
df_check_promos = spark.read.table("silver.promos_clean")
df_check_promos.printSchema()
print(f"Total rows: {df_check_promos.count()}")
df_check_promos.show(10, truncate=False)

print("\n" + "=" * 80)
print("SALES_CLEAN TABLE")
print("=" * 80)
df_check_sales = spark.read.table("silver.sales_clean")
df_check_sales.printSchema()
print(f"Total rows: {df_check_sales.count()}")
df_check_sales.show(10, truncate=False)

# ── Promotion Impact Analysis ──────────────────────────────────────────────
print("\n" + "=" * 80)
print("PROMOTION IMPACT ANALYSIS")
print("=" * 80)

# Read silver tables
df_sales = spark.read.table("silver.sales_clean")
df_promos = spark.read.table("silver.promos_clean")

# LEFT JOIN sales with promos on order_number and product_id = promo_item
df_sales_with_promos = (
    df_sales.alias("sales")
    .join(
        df_promos.alias("promos"),
        (F.col("sales.order_number") == F.col("promos.order_number")) & 
        (F.col("sales.product_id") == F.col("promos.promo_item")),
        "left"
    )
    .select(
        F.col("sales.*"),
        F.col("promos.promo_id"),
        F.col("promos.promo_disc")
    )
)

# Add is_promoted column
df_sales_with_promos = df_sales_with_promos.withColumn(
    "is_promoted",
    F.when(F.col("promo_id").isNotNull(), True).otherwise(False)
)

# Group by is_promoted and calculate metrics
df_promo_impact = (
    df_sales_with_promos
    .groupBy("is_promoted")
    .agg(
        F.sum(F.col("price") * F.col("qty")).alias("total_revenue"),
        F.sum("qty").alias("total_units_sold"),
        F.countDistinct("order_number").alias("total_orders"),
        (F.sum(F.col("price") * F.col("qty")) / F.countDistinct("order_number")).alias("avg_revenue_per_order")
    )
    .orderBy("is_promoted", ascending=False)
)

# Show results
df_promo_impact.show(truncate=False)

# Calculate promotion uplift
promoted = df_promo_impact.filter("is_promoted = true").first()
not_promoted = df_promo_impact.filter("is_promoted = false").first()

if promoted and not_promoted:
    revenue_uplift = ((promoted["avg_revenue_per_order"] - not_promoted["avg_revenue_per_order"]) / not_promoted["avg_revenue_per_order"]) * 100
    print(f"\nPromotion Uplift:")
    print(f"  Avg Revenue per Order (Promoted):     ${promoted['avg_revenue_per_order']:.2f}")
    print(f"  Avg Revenue per Order (Not Promoted): ${not_promoted['avg_revenue_per_order']:.2f}")
    print(f"  Revenue Uplift:                        {revenue_uplift:.2f}%")
