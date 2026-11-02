from pyspark.sql import functions as sf

result = (
  experiments
  .groupBy("exp_name")
  .agg(
    sf.countDistinct(
      sf.when(
        sf.col("variant") == "control",
        sf.col("user_id")
      )
    ).alias("control_users"),

    sf.countDistinct(
      sf.when(
        sf.col("variant") != 'control',
        sf.col("user_id")
      )
    ).alias("treatment_users"),

    sf.countDistinct(
      sf.when(
        sf.col("variant") == "holdout",
        sf.col("user_id")
      )
    ).alias("holdout_users")
  )
  .withColumn(
    "treatment_to_control_ratio",
    sf.when(
      sf.col("control_users") == F.col("holdout_users"),
      sf.lit(None).cast("double")
    ).otherwise(
      sf.round(
        sf.col("treatment_users") / F.col("control_users"),
        3
      )
    )
  )
  .select(
    "exp_name",
    "control_users",
    "treatment_users",
    "treatment_to_control_ratio"
  )
  .orderBy("exp_name")
)

result.show()
