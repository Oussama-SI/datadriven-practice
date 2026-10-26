from pyspark.sql import functions as sf

data = spark.table("data_pipes")

#data.show(20)

result = (
  data
  .where(sf.col("rows_out") > sf.lit(3000))
  .groupBy(sf.col("pipe_name"))
  .agg(sf.avg(sf.col("rows_in")).alias("avg_input_rows")
    )
  )
