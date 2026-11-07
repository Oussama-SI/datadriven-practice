from pyspark.sql import functions as F

#cloud_costs.filter(F.col("bill_date").rlike("^2025")).show(30)

result = (
  cloud_costs
  .withColumn("fiscal_year", F.year(F.to_date("bill_date")))
  .groupBy("fiscal_year")
  .agg(
    F.sum("amount").alias("total_spend"),
    F.countDistinct("svc_name").alias("service_count")
    )
  )
