# Databricks notebook source
# ─────────────────────────────────────────────────────────────
# Bronze Layer
# Load raw SCADA telemetry from Unity Catalog Volume
# ─────────────────────────────────────────────────────────────

from pyspark.sql import functions as F

RAW_SCADA_PATH = "/Volumes/workspace/default/raw_data/synthetic_scada.csv"

raw_df = (
    spark.read
         .format("csv")
         .option("header", True)
         .option("inferSchema", True)
         .load(RAW_SCADA_PATH)
)

print("Schema")
raw_df.printSchema()

print("Sample records")
raw_df.show(10, truncate=False)

print(f"Total records : {raw_df.count():,}")
print(f"Distinct sites: {raw_df.select('site_id').distinct().count()}")

# COMMAND ----------

from pyspark.sql import functions as F

# ------------------------------------------------------------
# Bronze Layer
# Add ingestion metadata
# ------------------------------------------------------------

bronze_df = (
    raw_df
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source_file", F.lit(RAW_SCADA_PATH))
)

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.bronze_scada_readings")
)

print("Bronze Delta table created successfully.")
print(f"Rows written: {bronze_df.count():,}")

# COMMAND ----------

bronze = spark.table("workspace.default.bronze_scada_readings")

print(f"Rows: {bronze.count():,}")

bronze.printSchema()

display(bronze.limit(20))

# COMMAND ----------

# ── Cell 4: Verify Bronze table — check row counts per site and spot the anomaly ──

from pyspark.sql import functions as F

# Row counts per site — should be 96 per site
print("Rows per site:")
bronze.groupBy("site_id").count().orderBy("site_id").show()

# Check the anomaly is present — level above 2.0 with pump ON
print("Anomaly check — rapid level rise events:")
bronze.filter(
    (F.col("level_m") > 2.0) & (F.col("pump_status") == "ON")
).select(
    "site_id",
    "timestamp", 
    "level_m", 
    "pump_status"
).orderBy(
    "site_id", 
    "timestamp"
).show(truncate=False)