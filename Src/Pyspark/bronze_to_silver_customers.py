from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    trim,
    lower,
    upper,
    when,
    to_date,
    to_timestamp,
    current_date,
    lit
)


# ============================================================
# STEP 1 — CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Bronze_to_Silver")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)


# ============================================================
# STEP 2 — DEFINE INPUT PATH
# ============================================================

input_path = "data/bronze/customers/customers.csv"


# ============================================================
# STEP 3 — READ BRONZE DATA
# ============================================================

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)


# ============================================================
# STEP 4 — DISPLAY DATA
# ============================================================

df.show(20, truncate=False)


# ============================================================
# STEP 5 — DISPLAY SCHEMA
# ============================================================

df.printSchema()


# ============================================================
# STEP 6 — COUNT RECORDS
# ============================================================

total_count = df.count()

print("Total records:", total_count)


# ============================================================
# STEP 9 — BASIC DATA PROFILING
# ============================================================

print("Null values by column:")

for column in df.columns:

    null_count = (
        df.filter(col(column).isNull()).count()
    )

    print(column, "=", null_count)


# Check duplicate customer IDs

duplicate_count = (
    df.groupBy("customer_id")
      .count()
      .filter("count > 1")
      .count()
)

print("Duplicate customer_id records:", duplicate_count)


# ============================================================
# STEP 10 — DATA CLEANING AND STANDARDIZATION
# ============================================================

cleaned_df = (
    df

    # --------------------------------------------------------
    # Clean customer_id
    # --------------------------------------------------------

    .withColumn(
        "customer_id",
        when(
            col("customer_id").isNull()
            | (trim(col("customer_id")) == ""),
            None
        ).otherwise(
            trim(col("customer_id"))
        )
    )

    # --------------------------------------------------------
    # Clean first_name
    # --------------------------------------------------------

    .withColumn(
        "first_name",
        when(
            col("first_name").isNull()
            | (trim(col("first_name")) == ""),
            None
        ).otherwise(
            trim(col("first_name"))
        )
    )

    # --------------------------------------------------------
    # Clean last_name
    # --------------------------------------------------------

    .withColumn(
        "last_name",
        when(
            col("last_name").isNull()
            | (trim(col("last_name")) == ""),
            None
        ).otherwise(
            trim(col("last_name"))
        )
    )

    # --------------------------------------------------------
    # Clean email
    # --------------------------------------------------------

    .withColumn(
        "email",
        when(
            col("email").isNull()
            | (trim(col("email")) == ""),
            None
        ).otherwise(
            lower(trim(col("email")))
        )
    )

    # --------------------------------------------------------
    # Clean city
    # --------------------------------------------------------

    .withColumn(
        "city",
        when(
            col("city").isNull()
            | (trim(col("city")) == ""),
            None
        ).otherwise(
            trim(col("city"))
        )
    )

    # --------------------------------------------------------
    # Standardize gender
    # --------------------------------------------------------

    .withColumn(
        "gender",
        when(
            col("gender").isNull()
            | (trim(col("gender")) == ""),
            None
        ).otherwise(
            upper(trim(col("gender")))
        )
    )

    # --------------------------------------------------------
    # Convert date_of_birth to DATE
    # --------------------------------------------------------

    .withColumn(
        "date_of_birth",
        to_date(
            col("date_of_birth"),
            "yyyy-MM-dd"
        )
    )

    # --------------------------------------------------------
    # Convert created_at to TIMESTAMP
    # --------------------------------------------------------

    .withColumn(
        "created_at",
        to_timestamp(col("created_at"))
    )

    # --------------------------------------------------------
    # Convert modified_at to TIMESTAMP
    # --------------------------------------------------------

    .withColumn(
        "modified_at",
        to_timestamp(col("modified_at"))
    )
)


# ============================================================
# DISPLAY CLEANED DATA
# ============================================================

print("================================")
print("CLEANED DATA SAMPLE")
print("================================")

cleaned_df.show(
    20,
    truncate=False
)

cleaned_count = cleaned_df.count()

print("Cleaned record count:", cleaned_count)


# ============================================================
# STEP 11 — CREATE VALIDATION CONDITION
# ============================================================

valid_condition = (
    col("customer_id").isNotNull()
    & (trim(col("customer_id")) != "")

    & col("email").isNotNull()
    & (trim(col("email")) != "")

    & col("gender").isin("M", "F")

    & col("date_of_birth").isNotNull()
    & (col("date_of_birth") <= current_date())
)


# ============================================================
# CREATE VALID DATAFRAME
# ============================================================

valid_df = (
    cleaned_df
    .filter(valid_condition)
)


# ============================================================
# CREATE INVALID DATAFRAME
# ============================================================

invalid_df = (
    cleaned_df
    .filter(~valid_condition)
)


# ============================================================
# STEP 12 — ADD QUARANTINE REASON
# ============================================================

invalid_df = (
    invalid_df
    .withColumn(
        "validation_reason",

        when(
            col("customer_id").isNull(),
            lit("MISSING_CUSTOMER_ID")
        )

        .when(
            col("email").isNull(),
            lit("MISSING_EMAIL")
        )

        .when(
            col("gender").isNull()
            | (~col("gender").isin("M", "F")),
            lit("INVALID_GENDER")
        )

        .when(
            col("date_of_birth").isNull(),
            lit("INVALID_DATE_OF_BIRTH")
        )

        .when(
            col("date_of_birth") > current_date(),
            lit("FUTURE_DATE_OF_BIRTH")
        )

        .otherwise(
            lit("UNKNOWN")
        )
    )
)


# ============================================================
# DISPLAY INVALID DATA
# ============================================================

print("================================")
print("INVALID CUSTOMER SAMPLE")
print("================================")

invalid_df.show(
    10,
    truncate=False
)


# ============================================================
# STEP 14 — WRITE VALID CUSTOMERS TO SILVER
# ============================================================

silver_path = "data/silver/customers"

valid_df.write \
    .mode("overwrite") \
    .parquet(silver_path)


# ============================================================
# STEP 15 — WRITE INVALID RECORDS TO QUARANTINE
# ============================================================

quarantine_path = "data/quarantine/customers"

invalid_df.write \
    .mode("overwrite") \
    .parquet(quarantine_path)


# ============================================================
# STEP 16 — PROCESSING STATISTICS
# ============================================================

valid_count = valid_df.count()

invalid_count = invalid_df.count()


print("================================")
print("CUSTOMER PROCESSING SUMMARY")
print("================================")

print("Bronze records      :", total_count)
print("Cleaned records     :", cleaned_count)
print("Valid records       :", valid_count)
print("Invalid records     :", invalid_count)
print("Reconciliation      :", valid_count + invalid_count)


# ============================================================
# RECONCILIATION CHECK
# ============================================================

if total_count == valid_count + invalid_count:

    print("Reconciliation      : PASSED")

else:

    print("Reconciliation      : FAILED")


# ============================================================
# OUTPUT LOCATIONS
# ============================================================

print("================================")
print("OUTPUT LOCATIONS")
print("================================")

print("Silver      :", silver_path)
print("Quarantine  :", quarantine_path)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()