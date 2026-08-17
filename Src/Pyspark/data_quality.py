from pyspark.sql.functions import (
    col,
    trim,
    current_date
)

from pyspark.sql import SparkSession


# ============================================================
# DATA QUALITY FRAMEWORK
# ============================================================


# ============================================================
# 1. NULL VALIDATION
# ============================================================

def validate_not_null(df, column_name):

    invalid_df = df.filter(
        col(column_name).isNull()
        | (trim(col(column_name)) == "")
    )

    return invalid_df


# ============================================================
# 2. ALLOWED VALUES VALIDATION
# ============================================================

def validate_allowed_values(
    df,
    column_name,
    allowed_values
):

    invalid_df = df.filter(
        col(column_name).isNull()
        | (~col(column_name).isin(allowed_values))
    )

    return invalid_df


# ============================================================
# 3. DUPLICATE VALIDATION
# ============================================================

def validate_duplicates(df, column_name):

    duplicate_keys = (
        df.groupBy(column_name)
        .count()
        .filter(col("count") > 1)
        .select(column_name)
    )

    duplicate_df = (
        df.join(
            duplicate_keys,
            on=column_name,
            how="inner"
        )
    )

    return duplicate_df


# ============================================================
# 4. COUNT INVALID RECORDS
# ============================================================

def count_invalid_records(df):

    return df.count()


# ============================================================
# 5. EMAIL VALIDATION
# ============================================================

def validate_email(df, column_name):

    email_pattern = (
        r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    )

    invalid_df = df.filter(
        col(column_name).isNotNull()
        & (~col(column_name).rlike(email_pattern))
    )

    return invalid_df


# ============================================================
# 6. GENDER VALIDATION
# ============================================================

def validate_gender(df, column_name):

    invalid_df = df.filter(
        col(column_name).isNotNull()
        & (~col(column_name).isin("M", "F"))
    )

    return invalid_df


# ============================================================
# 7. DATE OF BIRTH VALIDATION
# ============================================================

def validate_dob(df, column_name):

    invalid_df = df.filter(
        col(column_name).isNull()
        | (col(column_name) > current_date())
    )

    return invalid_df


# ============================================================
# 8. LOAD VALIDATION RULES
# ============================================================

def load_validation_rules(spark, rules_path):

    rules_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(rules_path)
    )

    # IMPORTANT:
    # Return the DataFrame, NOT the path
    return rules_df


# ============================================================
# 9. RULE DISPATCHER
# ============================================================

def apply_validation_rule(
    df,
    column_name,
    rule
):

    if rule == "NOT_NULL":

        return validate_not_null(
            df,
            column_name
        )

    elif rule == "VALID_EMAIL":

        return validate_email(
            df,
            column_name
        )

    elif rule == "VALID_GENDER":

        return validate_gender(
            df,
            column_name
        )

    elif rule == "VALID_DOB":

        return validate_dob(
            df,
            column_name
        )

    else:

        raise ValueError(
            f"Unknown validation rule: {rule}"
        )


# ============================================================
# MAIN PROGRAM
# ============================================================

if __name__ == "__main__":

    spark = (
        SparkSession.builder
        .appName("DataQualityTest")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


    # ========================================================
    # LOAD VALIDATION RULES
    # ========================================================

    rules_path = (
        "data/Config/validation_rules.csv"
    )

    rules_df = load_validation_rules(
        spark,
        rules_path
    )


    # ========================================================
    # LOAD BRONZE CUSTOMER DATA
    # ========================================================

    customer_path = (
        "data/bronze/customers/customers.csv"
    )

    customers_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(customer_path)
    )


    # ========================================================
    # DISPLAY VALIDATION RULES
    # ========================================================

    print("===========================")
    print("VALIDATION RULES")
    print("===========================")

    rules_df.show(
        truncate=False
    )


    # ========================================================
    # DISPLAY RULE SCHEMA
    # ========================================================

    print("===========================")
    print("RULE SCHEMA")
    print("===========================")

    rules_df.printSchema()


    # ========================================================
    # TEST FIRST RULE
    # ========================================================

    first_rule = rules_df.first()

    dataset = first_rule["dataset"]
    column_name = first_rule["column_name"]
    rule = first_rule["rule"]


    print("================================")
    print("FIRST RULE TEST")
    print("================================")

    print("Dataset :", dataset)
    print("Column  :", column_name)
    print("Rule    :", rule)


    # ========================================================
    # APPLY FIRST RULE
    # ========================================================

    invalid_df = apply_validation_rule(
        customers_df,
        column_name,
        rule
    )


    # ========================================================
    # DISPLAY INVALID RECORDS
    # ========================================================

    print("Invalid Records:")

    invalid_df.show(
        truncate=False
    )


    # ========================================================
    # DISPLAY INVALID COUNT
    # ========================================================

    print(
        "Invalid Record Count:",
        count_invalid_records(invalid_df)
    )


    # ========================================================
    # STOP SPARK
    # ========================================================

    spark.stop()