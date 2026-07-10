# Databricks notebook source
# DBTITLE 1,Cell 1
# ─────────────────────────────────────────────────────────────
# Silver Layer, Cell 1
# Load and validate the Bronze SCADA Delta table before applying
# any Silver layer cleaning or transformation logic.
# ─────────────────────────────────────────────────────────────

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ── Table configuration ───────────────────────────────────────
# Store table names as constants so they can be updated in one
# place if the catalog or schema changes later.

BRONZE_TABLE = "workspace.default.bronze_scada_readings"
SILVER_TABLE = "workspace.default.silver_scada_readings"


# ── Expected Bronze schema ────────────────────────────────────
# These columns are required for the Silver transformations.
# The notebook stops early if any of them are missing.

REQUIRED_COLUMNS = {
    "site_id",
    "timestamp",
    "level_m",
    "flow_lps",
    "pressure_bar",
    "pump_status",
}


# ── Load Bronze Delta table ───────────────────────────────────
# Read the Bronze table into a Spark DataFrame.

bronze_df = spark.table(BRONZE_TABLE)


# ── Validate row count ────────────────────────────────────────
# count() triggers Spark to load the table.
# Store the result so the table is not counted repeatedly.

bronze_row_count = bronze_df.count()

if bronze_row_count == 0:
    raise ValueError(
        f"Bronze table contains no rows: {BRONZE_TABLE}"
    )

print(f"Bronze rows loaded: {bronze_row_count:,}")


# ── Validate required columns ─────────────────────────────────
# Compare the expected columns with the columns found in the
# Bronze table. Stop the notebook if the structure is incomplete.

missing_columns = REQUIRED_COLUMNS - set(bronze_df.columns)

if missing_columns:
    raise ValueError(
        "Bronze table is missing required columns: "
        f"{sorted(missing_columns)}"
    )

print("Required column check passed.")


# ── Inspect Bronze schema ─────────────────────────────────────
# Print the column names and Spark data types before transforming
# the data in the next cell.

bronze_df.printSchema()


# ── Preview Bronze data ───────────────────────────────────────
# Order the sample by site and timestamp so readings appear in
# the same sequence they were recorded.

display(
    bronze_df
    .orderBy("site_id", "timestamp")
    .limit(20)
)

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Silver Layer, Cell 2
# Clean and standardise the Bronze SCADA data, then use a
# PySpark window function to compare each reading with the
# previous reading from the same site.
# ─────────────────────────────────────────────────────────────


# ── Clean and standardise Bronze records ──────────────────────
# Apply basic Silver layer cleaning before using window functions.
# This prevents invalid or duplicate rows from affecting the
# previous reading calculations.

silver_base_df = (
    bronze_df

    # Cast fields to the expected data types in case the Bronze
    # schema changes or future source files contain text values.
    .withColumn(
        "timestamp",
        F.col("timestamp").cast("timestamp")
    )
    .withColumn(
        "level_m",
        F.col("level_m").cast("double")
    )
    .withColumn(
        "flow_lps",
        F.col("flow_lps").cast("double")
    )
    .withColumn(
        "pressure_bar",
        F.col("pressure_bar").cast("double")
    )

    # Remove extra spaces and standardise pump status values.
    .withColumn(
        "pump_status",
        F.upper(F.trim(F.col("pump_status")))
    )

    # Remove rows that cannot be used safely in later calculations.
    .filter(
        F.col("site_id").isNotNull()
        & F.col("timestamp").isNotNull()
        & F.col("level_m").isNotNull()
        & F.col("flow_lps").isNotNull()
        & F.col("pressure_bar").isNotNull()
        & (F.col("level_m") >= 0)
        & (F.col("flow_lps") >= 0)
        & (F.col("pressure_bar") >= 0)
        & F.col("pump_status").isin("ON", "OFF", "AUTO")
    )

    # Keep one reading for each site and timestamp combination.
    .dropDuplicates(["site_id", "timestamp"])
)


# ── Window specification ──────────────────────────────────────
# Partition by site_id so each site is analysed independently.
# Order by timestamp so each reading is compared with the
# previous reading from the same site.

window_spec = (
    Window
    .partitionBy("site_id")
    .orderBy("timestamp")
)


# ── Calculate previous reading values ─────────────────────────
# lag() returns the value from the previous row within each site.
# The first reading for each site will have no previous value.

silver_df = (
    silver_base_df

    # Add the previous timestamp and level for each site.
    .withColumn(
        "previous_timestamp",
        F.lag("timestamp", 1).over(window_spec)
    )
    .withColumn(
        "previous_level_m",
        F.lag("level_m", 1).over(window_spec)
    )

    # Calculate the number of minutes since the previous reading.
    # This makes the logic safe if readings are not always exactly
    # 15 minutes apart.
    .withColumn(
        "minutes_since_previous",
        (
            F.unix_timestamp("timestamp")
            - F.unix_timestamp("previous_timestamp")
        ) / 60
    )

    # Calculate the absolute level change since the previous reading.
    .withColumn(
        "level_change_m",
        F.round(
            F.col("level_m") - F.col("previous_level_m"),
            3
        )
    )

    # Calculate the rate of level change per minute.
    .withColumn(
        "level_rate_m_per_min",
        F.when(
            F.col("minutes_since_previous") > 0,
            F.round(
                F.col("level_change_m")
                / F.col("minutes_since_previous"),
                4
            )
        )
    )

    # Flag rises greater than 0.4 metres between consecutive readings.
    # The threshold is currently a project rule and can be moved to
    # configuration later if different sites need different limits.
    .withColumn(
        "rapid_rise_flag",
        F.when(
            F.col("level_change_m") > 0.4,
            True
        ).otherwise(False)
    )

    # Add an audit timestamp showing when the Silver transformation ran.
    .withColumn(
        "silver_processed_at",
        F.current_timestamp()
    )
)


# ── Verify Silver transformation results ──────────────────────
# Compare Bronze and Silver row counts to confirm how many records
# were removed during cleaning.

silver_row_count = silver_df.count()
removed_row_count = bronze_row_count - silver_row_count

print("Silver transformations applied")
print(f"Bronze rows:      {bronze_row_count:,}")
print(f"Silver rows:      {silver_row_count:,}")
print(f"Rows removed:     {removed_row_count:,}")

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Silver Layer, Cell 3
# Verify that the window function detected the rapid level rise
# injected into the synthetic SCADA data during Week 2.
# ─────────────────────────────────────────────────────────────


# ── Filter anomaly records ────────────────────────────────────
# Keep only rows where the level increased by more than the
# configured threshold compared with the previous reading.

anomalies_df = (
    silver_df
    .filter(F.col("rapid_rise_flag"))
    .select(
        "site_id",
        "timestamp",
        "previous_timestamp",
        "level_m",
        "previous_level_m",
        "level_change_m",
        "minutes_since_previous",
        "level_rate_m_per_min",
        "pump_status",
        "rapid_rise_flag",
    )
    .orderBy(
        "site_id",
        "timestamp",
    )
)


# ── Summarise detection results ───────────────────────────────
# Count the total number of flagged readings and the number
# detected for each site.

anomaly_count = anomalies_df.count()

print(f"Rapid rise events detected: {anomaly_count:,}")
print()

display(anomalies_df)


# ── Check detection by site ───────────────────────────────────
# Group the results by site to confirm that the injected anomaly
# was detected across all five monitoring sites.

anomalies_by_site_df = (
    anomalies_df
    .groupBy("site_id")
    .agg(
        F.count("*").alias("rapid_rise_events"),
        F.min("timestamp").alias("first_flagged_at"),
        F.max("timestamp").alias("last_flagged_at"),
        F.max("level_change_m").alias("largest_level_change_m"),
    )
    .orderBy("site_id")
)

display(anomalies_by_site_df)

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Silver Layer, Cell 4
# Add a more specific possible pump failure flag.
# This combines the rapid rise signal with pump status and
# high level conditions to reduce false positives.
# ─────────────────────────────────────────────────────────────


# ── Add possible pump failure logic ───────────────────────────
# A reading is flagged as a possible pump failure when:
# 1. The level rose by more than 0.4 metres
# 2. The pump status is ON
# 3. The current level is above 2.0 metres
#
# This does not prove that a pump has failed. It identifies
# readings that need further investigation.

silver_enriched_df = (
    silver_df

    # Flag readings where the current level is above the
    # project threshold of 2.0 metres.
    .withColumn(
        "high_level_flag",
        F.col("level_m") > 2.0
    )

    # Combine the rapid rise, pump status and high level rules.
    .withColumn(
        "possible_pump_failure_flag",
        F.when(
            F.col("rapid_rise_flag")
            & (F.col("pump_status") == "ON")
            & F.col("high_level_flag"),
            True
        ).otherwise(False)
    )
)


# ── Verify possible pump failure results ──────────────────────
# Keep only the rows that meet all three conditions so the
# result can be checked against the injected anomaly period.

possible_pump_failures_df = (
    silver_enriched_df
    .filter(F.col("possible_pump_failure_flag"))
    .select(
        "site_id",
        "timestamp",
        "previous_timestamp",
        "level_m",
        "previous_level_m",
        "level_change_m",
        "minutes_since_previous",
        "level_rate_m_per_min",
        "pump_status",
        "rapid_rise_flag",
        "high_level_flag",
        "possible_pump_failure_flag",
    )
    .orderBy(
        "site_id",
        "timestamp",
    )
)


# ── Summarise results ─────────────────────────────────────────
# Count the more specific anomaly candidates after the extra
# business rules have been applied.

possible_failure_count = possible_pump_failures_df.count()

print(
    f"Possible pump failure events detected: "
    f"{possible_failure_count:,}"
)

display(possible_pump_failures_df)

# ── Check possible pump failures by site ──────────────────────
# Group flagged events by site to confirm which monitoring
# locations met the combined detection rules.

possible_failures_by_site_df = (
    possible_pump_failures_df
    .groupBy("site_id")
    .agg(
        F.count("*").alias("possible_failure_events"),
        F.min("timestamp").alias("first_detected_at"),
        F.max("timestamp").alias("last_detected_at"),
        F.max("level_change_m").alias("largest_level_change_m"),
        F.max("level_m").alias("highest_level_m"),
    )
    .orderBy("site_id")
)

display(possible_failures_by_site_df)

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Silver Layer, Cell 5
# Save the enriched Silver DataFrame as Delta files in a Unity
# Catalog volume so the Gold layer can read the validated data.
# ─────────────────────────────────────────────────────────────


# ── Silver storage configuration ──────────────────────────────
# Store the output path as a constant so it can be reused in the
# Gold notebook and changed easily if the volume location moves.

SILVER_PATH = "/Volumes/workspace/default/raw_data/silver_scada"


# ── Write Silver Delta files ──────────────────────────────────
# Overwrite the existing Delta files so the notebook can be run
# repeatedly during development and always produces a fresh result.
#
# Partitioning is not used because the dataset is currently small.
# Small partitions can create unnecessary files and overhead.

(
    silver_enriched_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(SILVER_PATH)
)


# ── Verify the Silver write ───────────────────────────────────
# Read the Delta files back from storage to confirm the write
# completed successfully and the expected data is available.

silver_verify_df = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
)

silver_verify_row_count = silver_verify_df.count()
silver_verify_column_count = len(silver_verify_df.columns)

print("Silver Delta files written successfully")
print(f"Path:               {SILVER_PATH}")
print(f"Rows written:       {silver_verify_row_count:,}")
print(f"Columns written:    {silver_verify_column_count:,}")

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Silver Layer, Cell 6
# Verify that the saved Delta output contains the expected
# anomaly flags and higher priority pump failure candidates.
# ─────────────────────────────────────────────────────────────


# ── Check saved anomaly totals ────────────────────────────────
# Aggregate the Boolean flags after reading the data back from
# storage. This confirms that the results survived the Delta write.

silver_validation_df = (
    silver_verify_df
    .agg(
        F.count("*").alias("total_rows"),
        F.sum(
            F.col("rapid_rise_flag").cast("int")
        ).alias("rapid_rise_events"),
        F.sum(
            F.col("possible_pump_failure_flag").cast("int")
        ).alias("possible_pump_failure_events"),
        F.countDistinct("site_id").alias("site_count"),
    )
)

display(silver_validation_df)


# ── Display saved pump failure candidates ─────────────────────
# Show the higher priority records directly from the saved Delta
# output rather than from the in memory transformation DataFrame.

display(
    silver_verify_df
    .filter(F.col("possible_pump_failure_flag"))
    .select(
        "site_id",
        "timestamp",
        "level_m",
        "previous_level_m",
        "level_change_m",
        "level_rate_m_per_min",
        "pump_status",
        "possible_pump_failure_flag",
    )
    .orderBy("site_id", "timestamp")
)


# COMMAND ----------

