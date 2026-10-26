from pyspark.sql import functions as sf

orders = spark.table("orders")

#orders.show(20)

result = (
  orders
  .filter(sf.col("region").isNotNull())
  .groupBy(sf.col("region"))
  .agg(sf.round(
    100 * sf.sum(
            sf.when(
              sf.col("status") == sf.lit("Completed"), 1).otherwise(0)) 
    / sf.count("*")
    , 2).alias("completion_pct"))
  .orderBy("region")
  )
