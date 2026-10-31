from pyspark.sql import functions as sf
from pyspark.sql import Window
#ad_impressions.show(20)
#transactions.show(20)

transactions = spark.table("transactions")
users = transactions.select("user_id").distinct()

window = Window.partitionBy("user_id").orderBy("impression_time")

iresult = (
  ad_impressions
    .join(users, "user_id", "inner")
  .withColumn("rn", sf.row_number().over(window))
  .filter(sf.col("rn") == 1)
 # .first(sf.col("impression_time").over(window))
  .select("user_id", "ad_campaign", "impression_time")
)
