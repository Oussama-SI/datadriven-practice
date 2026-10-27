from pyspark.sql import functions as sf
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

rate_limit = spark.table("rate_limits")

"""@udf(returnType=StringType())
def get_summary(min_allowed, max_allowed, path):
  return str(f"Allowed > {min_allowed} AND Allowed <= {max_allowed} => Endpoint = {path}")

@udf(returnType=StringType())
def debug_summary(min_allowed, max_allowed, path):
  return repr((min_allowed, max_allowed, path))

gresult = (
  rate_limit
  .groupBy(sf.col("endpoint"))
  .agg(
    sf.min(sf.col("allowed")).alias("min_allowed"),
    sf.max(sf.col("allowed")).alias("max_allowed"),
    )
  )
result = gresult.withColumn(
  "summary",
  get_summary(
    gresult["min_allowed"],
  gresult["max_allowed"],
  gresult["endpoint"]))

result = rate_limit.groupBy("endpoint").agg(
  sf.min("allowed").alias("min_allowed"),
  sf.max("allowed").alias("max_allowed"),
  sf.concat(
    sf.lit("Allowed > "),
    sf.min("allowed"),
    sf.lit(" AND Allowed <= "),
    sf.max("allowed"),
    sf.lit(" => Endpoint = "),
    sf.col("endpoint"),
  ).alias("summary"),
)"""

result = (
  rate_limits
  .groupBy("endpoint")
  .agg(
    sf.min("allowed").alias("min_allowed"),
    sf.max("allowed").alias("max_allowed"),
  )
  .withColumn(
    "summary",
    sf.concat(
      sf.lit("Allowed > "),
      sf.col("min_allowed"),
      sf.lit(" AND Allowed <= "),
      sf.col("max_allowed"),
      sf.lit(" => Endpoint = "),
      sf.col("endpoint"),
    ),
  )
)

result.explain("extended")
