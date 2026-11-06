from pyspark.sql import functions as F
from pyspark.sql.window import Window

#repo_commits.show(20)

avg_window = Window.partitionBy()


result = (
  repo_commits
  .filter(F.col("message").isNotNull() & ~(F.trim("message") == ""))
  .withColumn("message", F.trim('message'))
  .withColumn("message_length", F.length("message"))
  .withColumn("avg", F.avg("message_length").over(avg_window))
  .filter(F.col("message_length") > F.col("avg"))
  .select("author", "message", "message_length")
  .orderBy(F.desc("message_length"))
  )
