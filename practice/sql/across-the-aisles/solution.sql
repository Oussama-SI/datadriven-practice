from pyspark.sql.functions import (
  col, lit, when, count_distinct,
  sum, sum_distinct, asc, desc
  )

transactions = spark.table("transactions")
products = spark.table("products")

#transactions.show(20)

"""
grouped_df = (
  transactions
    .join(products, on="product_id", how="inner")
    .groupby("user_id")
    .agg(count_distinct(products.category).alias("category_count"),
      sum_distinct(transactions.total_amount).alias("total")
      )
    .sort(desc("category_count"), "total")
)"""

result = (
  transactions
  .filter(col("total_amount") < 500)
  .join(products, on="product_id", how="inner")
  .groupBy("user_id")
  .agg(
    count_distinct("category").alias("category_count")
  )
  .filter(col("category_count") > 1)
  .orderBy(col("category_count").desc())
)
