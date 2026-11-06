from pyspark.sql import functions as F

#api_tokens.show(20)

result = (
  api_tokens
  .filter(F.col("issued").rlike("^2026"), F.col("status").ilike("active"))
  .agg(
      F.countDistinct("owner_id").alias("distinct_owners")
    )
  )
