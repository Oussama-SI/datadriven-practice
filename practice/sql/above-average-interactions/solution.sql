# from spark.sql.DataFrame import 
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Step 1: Count sessions for each user
user_session_counts = (
  user_sessions
  .groupBy("user_id")
  .agg(F.count("session_id").alias("total_sessions"))
)

# Step 2: Calculate the average session count across all users
avg_sessions = user_session_counts.agg(
  F.avg("total_sessions").alias("avg_sessions")
)

# Step 3: Keep users whose session count exceeds the average
above_average_users = (
  user_session_counts
  .crossJoin(avg_sessions)
  .filter(F.col("total_sessions") > F.col("avg_sessions"))
  .drop("avg_sessions")
)

# Step 4: Sort by sessions descending, then user_id ascending
result = above_average_users.orderBy(
  F.col("total_sessions").desc(),
  F.col("user_id").asc()
)

# Step 5: Display the leaderboard
result.show()
