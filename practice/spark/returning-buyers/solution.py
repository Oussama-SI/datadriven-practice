from pyspark.sql import functions as F


brought_back = (
  Window
  .partitionBy("user_id")
  .orderBy(F.to_date("transaction_date"))
  )

result = (
  transactions
  .withColumn("next_date", lead("transaction_date").over(brought_back))
  .where(F.date_diff("next_date", F.to_date("transaction_date")).between(1, 7))
  .select("user_id", "transaction_date")
  .orderBy("transaction_date")
  )
