import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType
)

from pyspark.sql import functions as F


# ==========================================================
# PATHS
# ==========================================================

INPUT_DIR = "data/stream/incoming"

CHECKPOINT_DIR = (
    "data/stream/checkpoint"
)

OUTPUT_DIR = (
    "outputs/streaming/processed"
)


os.makedirs(
    INPUT_DIR,
    exist_ok=True
)

os.makedirs(
    CHECKPOINT_DIR,
    exist_ok=True
)

os.makedirs(
    OUTPUT_DIR,
    exist_ok=True
)


# ==========================================================
# SPARK
# ==========================================================

spark = (
    SparkSession.builder
    .appName(
        "NYC-Taxi-Structured-Streaming"
    )
    .master("local[*]")
    .config(
        "spark.sql.shuffle.partitions",
        "4"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel(
    "WARN"
)


print("=" * 72)
print("NYC TAXI - STRUCTURED STREAMING")
print("=" * 72)


# ==========================================================
# STREAM SCHEMA
# ==========================================================

schema = StructType([

    StructField(
        "pickup_location",
        StringType(),
        True
    ),

    StructField(
        "dropoff_location",
        StringType(),
        True
    ),

    StructField(
        "passenger_count",
        LongType(),
        True
    ),

    StructField(
        "pickup_hour",
        IntegerType(),
        True
    ),

    StructField(
        "pickup_day_of_week",
        IntegerType(),
        True
    ),

    StructField(
        "is_weekend",
        IntegerType(),
        True
    ),

    StructField(
        "event_id",
        IntegerType(),
        True
    )
])


# ==========================================================
# READ STREAM
#
# maxFilesPerTrigger=1:
# mỗi micro-batch chỉ đọc 1 file JSON
# ==========================================================

stream_df = (
    spark.readStream
    .schema(schema)
    .option(
        "maxFilesPerTrigger",
        1
    )
    .json(
        INPUT_DIR
    )
)


stream_df = (
    stream_df
    .withColumn(
        "processed_at",
        F.current_timestamp()
    )
)


print("\nSTREAM SCHEMA:")

stream_df.printSchema()


# ==========================================================
# MICRO-BATCH PROCESSING
# ==========================================================

def process_batch(
    batch_df,
    batch_id
):

    print(
        "\n"
        + "=" * 72
    )

    print(
        f"MICRO-BATCH ID: {batch_id}"
    )

    print(
        "=" * 72
    )


    batch_df = batch_df.cache()


    row_count = (
        batch_df.count()
    )


    print(
        f"\nROWS RECEIVED: {row_count}"
    )


    if row_count == 0:

        print(
            "EMPTY MICRO-BATCH"
        )

        batch_df.unpersist()

        return


    # ------------------------------------------------------
    # SAMPLE
    # ------------------------------------------------------

    print(
        "\nSTREAM DATA SAMPLE:"
    )

    batch_df.select(

        "event_id",
        "pickup_location",
        "dropoff_location",
        "passenger_count",
        "pickup_hour",
        "pickup_day_of_week",
        "is_weekend",
        "processed_at"

    ).show(
        10,
        truncate=False
    )


    # ------------------------------------------------------
    # SIMPLE REAL-TIME ANALYSIS
    # ------------------------------------------------------

    print(
        "\nREAL-TIME ANALYSIS BY PICKUP HOUR:"
    )

    (
        batch_df
        .groupBy(
            "pickup_hour"
        )
        .agg(

            F.count("*").alias(
                "trip_count"
            ),

            F.avg(
                "passenger_count"
            ).alias(
                "avg_passenger_count"
            )

        )
        .orderBy(
            "pickup_hour"
        )
        .show(
            24,
            truncate=False
        )
    )


    # ------------------------------------------------------
    # SAVE EACH MICRO-BATCH
    # ------------------------------------------------------

    output_path = os.path.join(
        OUTPUT_DIR,
        f"batch_{batch_id}"
    )


    (
        batch_df
        .write
        .mode("overwrite")
        .parquet(
            output_path
        )
    )


    print(
        "\nMICRO-BATCH SAVED:"
    )

    print(
        output_path
    )


    print(
        f"\nMICRO-BATCH {batch_id} SUCCESS"
    )


    batch_df.unpersist()


# ==========================================================
# START STREAM
# ==========================================================

query = (

    stream_df
    .writeStream
    .foreachBatch(
        process_batch
    )
    .option(
        "checkpointLocation",
        CHECKPOINT_DIR
    )
    .trigger(
        processingTime="3 seconds"
    )
    .start()

)


print("\n" + "=" * 72)

print(
    "STREAMING STARTED"
)

print("=" * 72)

print(
    "Watching folder:"
)

print(
    INPUT_DIR
)

print(
    "\nWaiting for JSON files..."
)

print(
    "Press CTRL+C after all 5 batches are processed."
)


try:

    query.awaitTermination()

except KeyboardInterrupt:

    print(
        "\nSTOPPING STREAM..."
    )

    query.stop()

finally:

    spark.stop()


print("\n" + "=" * 72)

print(
    "STRUCTURED STREAMING STOPPED"
)

print("=" * 72)