import os
from pathlib import Path

from pyspark.sql import SparkSession
from pysparkling import H2OContext, H2OConf


# ----------------------------------------------------------
# FIX DUPLICATE SPARKLING WATER JAR ON WINDOWS
# ----------------------------------------------------------
submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "")

print("PYSPARK_SUBMIT_ARGS BEFORE FIX =", submit_args)

prefix = "--jars "
suffix = " pyspark-shell"

if submit_args.startswith(prefix) and submit_args.endswith(suffix):
    raw_jars = submit_args[len(prefix):-len(suffix)]

    jar_paths = [
        p.strip()
        for p in raw_jars.split(",")
        if p.strip()
    ]

    fixed_jars = []
    seen = set()

    for jar_path in jar_paths:
        if jar_path.startswith("file:"):
            jar_uri = jar_path
        else:
            jar_uri = Path(jar_path).resolve().as_uri()

        if jar_uri not in seen:
            seen.add(jar_uri)
            fixed_jars.append(jar_uri)

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--jars "
        + ",".join(fixed_jars)
        + " pyspark-shell"
    )

print(
    "PYSPARK_SUBMIT_ARGS AFTER FIX =",
    os.environ.get("PYSPARK_SUBMIT_ARGS")
)


print("=" * 70)
print("SPARKLING WATER - EXTERNAL BACKEND TEST")
print("=" * 70)

external_host = os.environ.get("H2O_EXTERNAL_HOST", "127.0.0.1")
external_port = int(os.environ.get("H2O_EXTERNAL_PORT", "54321"))
external_cloud = os.environ.get("H2O_EXTERNAL_CLOUD", "nyc_taxi_external")


spark = (
    SparkSession.builder
    .appName("NYC-Taxi-External-H2O-Test")
    .master("local[2]")
    .config("spark.driver.memory", "1g")
    .config("spark.executor.memory", "1g")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ----------------------------------------------------------
# CONNECT TO EXISTING EXTERNAL H2O CLUSTER
# ----------------------------------------------------------
conf = (
    H2OConf()
    .setExternalClusterMode()
    .useManualClusterStart()
    .setH2OCluster(external_host, external_port)
    .setCloudName(external_cloud)
)

hc = H2OContext.getOrCreate(conf)

print()
print("EXTERNAL H2O CONTEXT CONNECTED SUCCESSFULLY")
print(f"H2O CLUSTER: {external_host}:{external_port}")
print(f"CLOUD NAME: {external_cloud}")


# ----------------------------------------------------------
# SMALL SPARK -> H2O -> SPARK TEST
# ----------------------------------------------------------
spark_df = spark.range(1, 11).withColumnRenamed("id", "value")

print()
print("SPARK ROW COUNT:", spark_df.count())

h2o_frame = hc.asH2OFrame(
    spark_df,
    "external_backend_demo"
)

spark_back = hc.asSparkFrame(h2o_frame)

print("H2O -> SPARK ROW COUNT:", spark_back.count())

spark_back.show()


print()
print("=" * 70)
print("EXTERNAL BACKEND TEST SUCCESS")
print("=" * 70)

spark.stop()
