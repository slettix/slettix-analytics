"""
Smoke test: verify Spark can write and read a Delta table on MinIO.

Run from inside the spark-master container:
  spark-submit /opt/spark/jobs/smoke_test.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TARGET_PATH = "s3a://bronze/smoke-test"

spark = (
    SparkSession.builder
    .appName("smoke-test")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Write
print(">> Writing Delta table...")
df = spark.createDataFrame(
    [(1, "alpha"), (2, "beta"), (3, "gamma")],
    ["id", "name"],
)
df.write.format("delta").mode("overwrite").save(TARGET_PATH)
print(f"   Wrote {df.count()} rows to {TARGET_PATH}")

# Read back
print(">> Reading Delta table...")
df_read = spark.read.format("delta").load(TARGET_PATH)
df_read.show()

assert df_read.count() == 3, "Row count mismatch!"
print(">> Smoke test PASSED")

spark.stop()
