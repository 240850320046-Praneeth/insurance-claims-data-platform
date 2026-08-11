from pyspark.sql.functions import *

from pyspark.sql import SparkSession

# ================================
# Data Quality Check
# ================================


def validate_not_null(df, column_name):

    invalid_df = df.filter(col(column_name).isNull() | (trim(col(column_name)) == ""))

    return invalid_df


def validate_allowed_values(df, column_names, allowd_values):

    invalid_df = df.filter(
        col(column_names).isNull() | (~col(column_names).isin(allowd_values))
    )

    return invalid_df


def validate_not_feature(df, column_name):

    invalid_df = df.filter(
        col(column_name).isNull() | (col(column_name) > current_date())
    )

    return invalid_df


def validate_duplicates(df, column_name):

    duplicate_keys = (
        df.groupBy(column_name).count().filter(col("count") > 1).select(column_name)
    )

    duplicate_df = df.join(duplicate_keys, on=column_name, how="inner")

    return duplicate_df


def count_invalid_records(df):

    return df.count()


if __name__ == "__main__":
    from pyspark.sql import *

    spark = (
        SparkSession.builder.appName("DataQualityTest")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    input_path = "../../data/bronze/customers/customers.csv"

    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

    print("===========================")
    print("DATA QUALITY TEST")
    print("===========================")

    invalid_email = validate_not_null(df, "email")

    print("Invalid Email Records : " )
    invalid_email.show()

    invalid_gender = validate_allowed_values(df, "gender", ["M", "F"])

    print("Invalid Gender Records : ")
    
    invalid_gender.show()

    invalid_dob = validate_not_feature(df, "date_of_birth")

    print("Invalid DOB Records : " )
    
    invalid_dob.show()

    duplicate_customers = validate_duplicates(df, "customer_id")

    print("Duplicate Records : " )
    
    duplicate_customers.show()

    spark.stop()
