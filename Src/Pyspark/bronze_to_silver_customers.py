from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (
    SparkSession.builder.appName("Bronze_to_Silver")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)

input_path = "data/bronze/customers/customers.csv"

df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)


df.show(20, truncate=False)


df.printSchema()


print("total_records ", df.count())


print("Null values by column: ")

for column in df.columns:
    null_count = df.filter(df[column].isNull()).count()
    print(column, " = ", null_count)

duplicate_count = df.groupBy("customer_id").count().filter("count>1").count()

print("Duplicates cust_id : ", duplicate_count)

# valid_cust_id=df.filter(col("customer_id").isNotNull())

# valid_email=df.filter(col("email").isNotNull()&col("email") != "")


# vaild_gender=df.filter(col("gender").isin("M","F"))

# valid_DOB=df.filter(col("date_of_birth")<=current_date())

valid_condition = (
    col("customer_id").isNotNull()
    & col("email").isNotNull()
    & (col("email") != "")
    & col("gender").isin("M", "F")
    & (col("date_of_birth") <= current_date())
)

valid_df = df.filter(valid_condition)
invalid_df = df.filter(~valid_condition)

invalid_df = df.withColumn(
    "Validation_reason",
    when(col("customer_id").isNull(), lit("Missing_Cust_ID"))
    .when(col("email").isNull() | (col("email") == ""), lit("Missing_Email"))
    .when(~col("Gender").isin("M", "F"), lit("Invalid_Gender"))
    .when(col("date_of_birth") > current_date(), lit("Future_date"))
    .otherwise(lit("Unknown")),
).filter(~valid_condition)

silver_path = "data/silver/customers"

valid_df.write.mode("overwrite").parquet(silver_path)

quarintine_path = "data/quarantine/customers"

invalid_df.write.mode("overwrite").parquet(quarintine_path)


total_count = df.count()
valid_count = valid_df.count()
invalid_count = invalid_df.count()

print("====Customer Processing Summary===")
print("Bronze records      :", total_count)
print("Valid records       :", valid_count)
print("Invalid records     :", invalid_count)
print("Reconciliation      :", valid_count + invalid_count)

spark.stop()
