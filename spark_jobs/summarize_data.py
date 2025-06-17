from pyspark.sql import SparkSession
from pyspark.sql.functions import col,avg,count, sum as _sum, desc
from pyspark.sql.types import IntegerType

spark = SparkSession.Builder.appName("UtubeDataAggregator").getOrCreate()

df = spark.read.parquet("data/processed/")

videos_per_category = df.groupBy("category_id").agg(count("*").alias("total_videos"))

views_per_category = df.groupBy("category_id").agg(_sum("view_count").alias("total_views"))

avg_likes_per_category = df.groupBy("category_id").agg(avg("like_count").alias("avg_likes"))

avg_duration_per_category = df.groupBy("category_id").agg(avg("duration_sec").alias("avg_duration_sec"))

top_videos = df.select(['title','channel_title','view_count']) \
                .orderBy(desc('view_count')) \
                .limit(5)


videos_per_category.write.mode("overwrite").json("data/summary/videos_per_category/")
views_per_category.write.mode("overwrite").json("data/summary/views_per_category/")
avg_likes_per_category.write.mode("overwrite").json("data/summary/avg_likes_per_category/")
avg_duration_per_category.write.mode("overwrite").json("data/summary/avg_duration_per_category/")
top_videos.write.mode("overwrite").json("data/summary/top_videos/")

print("Summary reports written to 'data/summary/' folder.")
spark.stop()