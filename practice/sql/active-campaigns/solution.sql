from pyspark.sql.functions import col, when, round, count, sum, desc

result = (
  ad_impressions
  .groupBy("ad_campaign")
  .agg(
    count(col("impression_id")).alias("impressions"),
    sum(col("revenue")).alias("total_revenue"),
    round(100.0 * sum(when(col("clicked") > 0, 1).otherwise(0)) / count(col("clicked")), 1)
    .alias("ctr")
    )
  .filter(col("impressions") > 15)
  .orderBy(desc("ctr"), "ad_campaign")
  )
