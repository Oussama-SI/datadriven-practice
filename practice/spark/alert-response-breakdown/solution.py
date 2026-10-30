from pyspark.sql import functions as sf

# alert_events.show(20)
#alert_events 

result = (
  alert_events
  .groupBy(sf.col("svc_name"))
  .agg(
    sf.count(sf.col("alert_id")).alias("total_alerts"),
    sf.count_if(sf.lower(sf.col("severity")) == "critical").alias("critical_amount"),
    sf.count_if(sf.col("severity").ilike("high")).alias("high_count"),
    sf.count_if(sf.col("ack_by").isNull()).alias("unacked_count"),
    sf.round(
      sf.count("alert_id") / sf.countDistinct("status"),
      2
     ).alias("avg_per_status")
    )
  .orderBy(sf.desc("total_alerts"), "svc_name")
 )
