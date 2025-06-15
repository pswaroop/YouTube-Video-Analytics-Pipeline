from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, to_timestamp
from pyspark.sql.types import StructType,StringType,ArrayType,StructField
import sys
from datetime import datetime

spark = SparkSession.builder.appName('UtubeTrendingDataCleaner')\
.getOrCreate()

INPUT_PATH = "data/raw/"
OUTPUT_PATH = "data/cleaned/"

TODAY = datetime.utcnow().strftime("%y-%m-%d")

schema = StructType([
    StructField("video_id",StringType(),False),
    StructField("title",StringType()),
    StructField("channel_title",StringType()),
    StructField("published_at",StringType()),
    StructField("category_id",StringType()),
    StructField("tags",ArrayType(StringType())),
    StructField("view_count",StringType()),
    StructField("like_count",StringType()),
    StructField("comment_count",StringType()),
    StructField("duration",StringType()),
    StructField("trending_date",StringType()),
])

dataframe = spark.read.json(path=INPUT_PATH,schema=schema,multiLine=True)

df_cleaned = dataframe \
.withColumn("published_at",to_timestamp("published_at")) \
.withColumn("category_id",col("category_id").cast("int")) \
.withColumn("view_count",col("view_count").cast("long")) \
.withColumn("like_count",col("like_count").cast("long")) \
.withColumn("comment_count",col("comment_count").cast("long")) \
.withColumn("trending_date",to_timestamp("trending_date")) \
.withColumn("tag",explode("tags")) \
.filter(col("view_count")>0)\
.na.drop(subset=["video_id","title","channel_title"])


df_cleaned.write.mode("overwrite").partitionBy("trending_date").parquet(OUTPUT_PATH)

print(f"Cleaned Data of {TODAY} written to: {OUTPUT_PATH} successfully")