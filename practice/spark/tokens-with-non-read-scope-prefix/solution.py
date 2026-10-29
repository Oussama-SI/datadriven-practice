from pyspark.sql import functions as sf

#api_tokens.show(20)
api_tokens = spark.table("api_tokens")

result = (
  api_tokens
  .filter(~sf.col("scope").startsWith("read"))
  .agg(
    sf.countDistinct("owner_id").alias("non_read_owner_count")
    )
  )
