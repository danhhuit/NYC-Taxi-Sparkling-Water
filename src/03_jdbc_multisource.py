import getpass

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


# ==========================================================
# 1. POSTGRESQL PASSWORD
# ==========================================================

db_password = getpass.getpass(
    "Nhap PostgreSQL password: "
)


# ==========================================================
# 2. CREATE SPARK SESSION
# ==========================================================

spark = (
    SparkSession.builder
    .appName("NYC-Taxi-JDBC-MultiSource")
    .master("local[*]")

    # PostgreSQL JDBC Driver
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.13"
    )

    # Giữ Ivy/Maven cache trên ổ D:
    .config(
        "spark.jars.ivy",
        "D:/BigData/ivy-cache"
    )

    .config(
        "spark.sql.shuffle.partitions",
        "8"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("NYC TAXI - MULTI-SOURCE JDBC DEMO")
print("=" * 70)


# ==========================================================
# 3. LOAD PARQUET SOURCE
# ==========================================================

taxi_df = spark.read.parquet(
    "data/processed/taxi_features_demo.parquet"
)


print("\nPARQUET SOURCE:")
print("Taxi rows:", taxi_df.count())

taxi_df.select(
    "PULocationID",
    "DOLocationID",
    "pickup_borough",
    "pickup_zone",
    "pickup_hour",
    "trip_duration_minutes"
).show(5, truncate=False)


# ==========================================================
# 4. LOAD POSTGRESQL VIA JDBC
# ==========================================================

# jdbc_url = (
#     "jdbc:postgresql://localhost:5432/nyc_taxi_db"
# )
jdbc_url = (
    "jdbc:postgresql://127.0.0.1:5432/nyc_taxi_db"
)
zone_stats_df = (
    spark.read
    .format("jdbc")
    .option(
        "url",
        jdbc_url
    )
    .option(
        "dbtable",
        "public.zone_statistics"
    )
    .option(
        "user",
        "postgres"
    )
    .option(
        "password",
        db_password
    )
    .option(
        "driver",
        "org.postgresql.Driver"
    )
    .load()
)


print("\nPOSTGRESQL JDBC SOURCE:")

zone_stats_df.printSchema()

zone_stats_df.show(
    truncate=False
)


# ==========================================================
# 5. MULTI-SOURCE JOIN
# ==========================================================

multi_df = (
    taxi_df
    .join(
        zone_stats_df,
        taxi_df.PULocationID
        == zone_stats_df.zone_id,
        "left"
    )
    .drop("zone_id")
)


# ==========================================================
# 6. CHECK RESULT
# ==========================================================

print("\nMULTI-SOURCE JOIN SAMPLE:")

multi_df.select(
    "PULocationID",
    "pickup_borough",
    "pickup_zone",
    "pickup_hour",

    "zone_type",
    "congestion_level",
    "priority_score",

    "trip_duration_minutes"
).show(
    20,
    truncate=False
)


# ==========================================================
# 7. COUNT MATCHED JDBC RECORDS
# ==========================================================

total_rows = multi_df.count()

matched_rows = (
    multi_df
    .filter(
        col("zone_type").isNotNull()
    )
    .count()
)


print("\nTOTAL TAXI ROWS:")
print(total_rows)

print("\nROWS MATCHED WITH POSTGRESQL:")
print(matched_rows)


# ==========================================================
# 8. SPARK SQL OVER MULTI-SOURCE DATA
# ==========================================================

multi_df.createOrReplaceTempView(
    "taxi_multisource"
)


print("\nJDBC + PARQUET ANALYSIS:")

spark.sql("""
    SELECT
        zone_type,
        congestion_level,
        COUNT(*) AS total_trips,
        ROUND(
            AVG(trip_duration_minutes),
            2
        ) AS avg_duration
    FROM taxi_multisource
    WHERE zone_type IS NOT NULL
    GROUP BY
        zone_type,
        congestion_level
    ORDER BY total_trips DESC
""").show(
    truncate=False
)


# ==========================================================
# 9. SAVE MULTI-SOURCE DATA
# ==========================================================

OUTPUT_PATH = (
    "data/processed/"
    "taxi_features_multisource.parquet"
)

multi_df.write.mode(
    "overwrite"
).parquet(
    OUTPUT_PATH
)


print("\nOUTPUT SAVED:")
print(OUTPUT_PATH)

print("=" * 70)
print("JDBC MULTI-SOURCE SUCCESS")
print("=" * 70)


spark.stop()