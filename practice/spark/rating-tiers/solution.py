from pyspark.sql import functions as F
from pyspark.sql.window import Window

#products.show(30)

pos = Window.partitionBy("category").orderBy(F.floor("rating").desc())

result = (
  products
  .filter(F.col("rating").isNotNull())
  .withColumn("position",  dense_rank().over(pos))
  .select("product_name", "category", "rating", "position")
  )
