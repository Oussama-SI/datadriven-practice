from pyspark.sql import functions as sf

#transactions.show(20)
users = spark.table("users")

result = (
  transactions
  .filter(sf.col('transaction_date').rlike("^2026-04-[0-9]{2}$"))
  .join(users, on="user_id", how="inner")
  .where(sf.col("account_status") == sf.lit("active"))
  .agg(
    sf.countDistinct(sf.col("user_id")).alias("active_users_with_transactions")
    )
  )
