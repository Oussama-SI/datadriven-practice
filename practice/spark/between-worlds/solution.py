from pyspark.sql import functions as F

# Join sessions with known devices
sessions_with_devices = user_sessions.join(
  devices,
  on="device_id",
  how="inner"
)

users = (
  sessions_with_devices
  .groupBy("user_id")
  .agg(
    F.countDistinct("device_type").alias("devices")
  )
  .filter(F.col("devices") > 1)
  .select("user_id")
)

# Return all qualifying sessions
result = (
  sessions_with_devices
  .join(users, on="user_id", how="inner")
  .select(
    "user_id",
    "os_name",
    "device_type",
    "session_start"
  )
)
