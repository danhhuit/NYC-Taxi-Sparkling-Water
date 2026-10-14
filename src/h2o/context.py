import os
from pathlib import Path

import pysparkling
from pyspark.sql import SparkSession
from pysparkling import H2OContext, H2OConf


# ==========================================================
# WINDOWS FIX FOR PYSPARKLING JAR PATH
# ==========================================================

submit_args = os.environ.get(
    "PYSPARK_SUBMIT_ARGS",
    ""
)

prefix = "--jars "
suffix = " pyspark-shell"

if (
    submit_args.startswith(prefix)
    and submit_args.endswith(suffix)
):
    jar_path = submit_args[
        len(prefix):-len(suffix)
    ]

    # D:\BigData\...
    # -> file:///D:/BigData/...
    jar_uri = Path(jar_path).as_uri()

    os.environ[
        "PYSPARK_SUBMIT_ARGS"
    ] = f"--jars {jar_uri} pyspark-shell"


print(
    "PYSPARK_SUBMIT_ARGS =",
    os.environ.get("PYSPARK_SUBMIT_ARGS")
)


# ==========================================================
# CREATE SPARK SESSION
# ==========================================================

spark = (
    SparkSession.builder
    .appName("NYC-Taxi-H2O-Context")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("SPARKLING WATER - H2O INTERNAL BACKEND")
print("=" * 70)


# ==========================================================
# H2O INTERNAL BACKEND
# ==========================================================

conf = H2OConf().setInternalClusterMode()

hc = H2OContext.getOrCreate(conf)


print("\nH2O CONTEXT CREATED SUCCESSFULLY")

print("\nSpark version:")
print(spark.version)

print("\nSpark master:")
print(spark.sparkContext.master)

print("\nH2OContext:")
print(hc)


print("=" * 70)
print("H2O INTERNAL BACKEND SUCCESS")
print("=" * 70)


spark.stop()