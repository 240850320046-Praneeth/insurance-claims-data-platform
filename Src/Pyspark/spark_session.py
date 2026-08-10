from pyspark.sql import SparkSession

spark= SparkSession.builder\
    .appName("Insurance Project")\
        .master("local[*]")\
            .config('spark.driver.memory','2g')\
            .getOrCreate()

print("Spark Version : ",spark.version)

spark.stop()