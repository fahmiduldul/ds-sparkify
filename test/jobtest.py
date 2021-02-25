from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t

spark = SparkSession.builder.appName('test').getOrCreate()

df = spark.read.json('gs://udacity-dsnd/sparkify_event_data.json')

print(df.head(5))

spark.stop()