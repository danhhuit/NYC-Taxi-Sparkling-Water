import os
import json

from pyspark.sql import SparkSession


# ==========================================================
# PATHS
# ==========================================================

SOURCE_PATH = (
    "data/processed/"
    "taxi_batch_input.parquet"
)

STAGING_DIR = (
    "data/stream/staging"
)

INCOMING_DIR = (
    "data/stream/incoming"
)


# ==========================================================
# CREATE DIRECTORIES
# ==========================================================

os.makedirs(
    STAGING_DIR,
    exist_ok=True
)

os.makedirs(
    INCOMING_DIR,
    exist_ok=True
)


# Remove old demo stream files only
for folder in [
    STAGING_DIR,
    INCOMING_DIR
]:

    for filename in os.listdir(folder):

        if (
            filename.startswith("batch_")
            and filename.endswith(".json")
        ):

            os.remove(
                os.path.join(
                    folder,
                    filename
                )
            )


# ==========================================================
# SPARK
# ==========================================================

spark = (
    SparkSession.builder
    .appName(
        "NYC-Taxi-Prepare-Streaming-Files"
    )
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel(
    "WARN"
)


print("=" * 70)
print("NYC TAXI - PREPARE STREAMING FILES")
print("=" * 70)


# ==========================================================
# LOAD SOURCE
# ==========================================================

df = spark.read.parquet(
    SOURCE_PATH
)


# Only 500 records for streaming demo
rows = (
    df.select(
        "pickup_location",
        "dropoff_location",
        "passenger_count",
        "pickup_hour",
        "pickup_day_of_week",
        "is_weekend"
    )
    .limit(500)
    .collect()
)


print(
    "\nTOTAL STREAM DEMO RECORDS:",
    len(rows)
)


# ==========================================================
# CREATE 5 MICRO-BATCH FILES
# 100 records / file
# ==========================================================

batch_size = 100
number_of_batches = 5


for batch_index in range(
    number_of_batches
):

    start = (
        batch_index
        * batch_size
    )

    end = (
        start
        + batch_size
    )

    batch_rows = rows[
        start:end
    ]


    filename = (
        f"batch_"
        f"{batch_index + 1:02d}.json"
    )

    output_path = os.path.join(
        STAGING_DIR,
        filename
    )


    with open(
        output_path,
        "w",
        encoding="utf-8"
    ) as f:

        for local_index, row in enumerate(
            batch_rows,
            start=1
        ):

            record = row.asDict()

            record[
                "event_id"
            ] = (
                batch_index
                * batch_size
                + local_index
            )

            f.write(
                json.dumps(
                    record,
                    ensure_ascii=False
                )
                + "\n"
            )


    print(
        f"CREATED: {output_path}"
        f" -> {len(batch_rows)} records"
    )


# ==========================================================
# SUMMARY
# ==========================================================

print("\nSTAGING FILES:")

for filename in sorted(
    os.listdir(
        STAGING_DIR
    )
):

    print(
        " -",
        filename
    )


print(
    "\nINCOMING FOLDER READY:"
)

print(
    INCOMING_DIR
)


print("\n" + "=" * 70)
print("STREAM FILE PREPARATION SUCCESS")
print("=" * 70)


spark.stop()