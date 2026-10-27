from pyspark.sql import functions as sf

ad_impressions = spark.table("ad_impressions")
page_views = spark.table("page_views")

#ad_impressions.show(20)
#page_views.show(20)

result = (
  ad_impressions
  .join(page_views, on="user_id", how="right")
  .filter(ad_impressions.user_id.isNotNull())
  .groupBy(sf.col("ad_campaign"))
  .agg(
    round(
    sf.sum(when(sf.col("clicked") > sf.lit(0), 1).otherwise(0))
    * 100.0 /
    count("*"),
      2).alias("clicked_pct")
    )
  .orderBy(sf.desc("clicked_pct"), "ad_campaign")
  )
