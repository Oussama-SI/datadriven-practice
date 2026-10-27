from pyspark.sql import window, functions as F


# transactions_unit = transactions.withColumn(
#  "unit_price", transactions.total_amount / transactions.quantity
#).select("product_id", "unit_price")

trans_base = transactions.groupBy(
  "product_id").agg(
  F.min("total_amount").alias("base_price")
).cache()

avg_price = trans_base.agg(
  F.avg("base_price").alias("avg_base_price")
).first()["avg_base_price"]

result = (
  trans_base
  .filter(F.col("base_price") > F.lit(avg_price))
  .select("product_id", "base_price")
)

result.show()
