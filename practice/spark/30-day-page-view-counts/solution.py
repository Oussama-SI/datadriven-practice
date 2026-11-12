from pyspark.sql import functions as F

#page_views.show(30)


result = (
  page_views
  .filter(F.col("viewed_at").between("2025-01-01", "2025-12-29"))
  .groupBy("user_id")
  .agg(
    F.count("view_id").alias("total_views")
    )
  )
