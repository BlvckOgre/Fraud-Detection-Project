from pyspark.sql import SparkSession
from pyspark.sql.function import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder\
    .appName("FraudDetection")\
    ,getOrCreate()

schema = StructType()\
    .add("user_id", StringType)\
    .add("amount", DoubleTple)\
    .add("location", StringType)\
    .add("ip", StringType)\
    .add("timestamp", StringType)

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bank_transactions") \
    .load()

df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

df_parsed.printSchema()