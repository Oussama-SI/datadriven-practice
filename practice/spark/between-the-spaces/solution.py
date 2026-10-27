from pyspark.sql.functions import col, when, length, regexp_replace

#chat_msgs.show(10)
result = chat_msgs.select(
  "msg_id",
  when(
    col("content").isNull(), None
  ).otherwise(
    length("content") - length(regexp_replace("content", " ", "")) + 1
  ).alias("word_count")
)
