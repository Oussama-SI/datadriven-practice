from pyspark.sql import functions as sf

#alert_events = spark.table("alert_events")

ressult = spark.sql("""
  SELECT *
FROM alert_events
WHERE LOWER(severity) IN ('high', 'critical')
  AND fired_at >= '2026-01-01 00:00:00'
  AND fired_at <  '2027-01-01 00:00:00';
  """)
