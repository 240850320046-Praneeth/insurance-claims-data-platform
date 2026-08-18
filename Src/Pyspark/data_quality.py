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
# 1A. NULL INVALID CONDITION
# ============================================================

def not_null_invalid_condition(column_name):

    return (
        col(column_name).isNull()
        | (trim(col(column_name)) == "")
    )


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
# 5A. EMAIL INVALID CONDITION
# ============================================================

def email_invalid_condition(column_name):

    email_pattern = (
        r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    )

    return (
        col(column_name).isNotNull()
        & (~col(column_name).rlike(email_pattern))
    )


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
# 6A. GENDER INVALID CONDITION
# ============================================================

def gender_invalid_condition(column_name):

    return (
        col(column_name).isNotNull()
        & (~col(column_name).isin("M", "F"))
    )


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
# 7A. DATE OF BIRTH INVALID CONDITION
# ============================================================

def dob_invalid_condition(column_name):

    return (
        col(column_name).isNull()
        | (col(column_name) > current_date())
    )


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

    elif rule == "DUPLICATE":

        return validate_duplicates(
            df,
            column_name
        )

    else:

        raise ValueError(
            f"Unknown validation rule: {rule}"
        )


# ============================================================
# 10. APPLY ALL VALIDATION RULES
# ============================================================

def apply_validation_rules(df, rules_df):

    results = []

    # rules_df is a small configuration DataFrame
    for rule_row in rules_df.collect():

        dataset = rule_row["dataset"]
        column_name = rule_row["column_name"]
        rule = rule_row["rule"]

        print()
        print("========================================")
        print("EXECUTING VALIDATION RULE")
        print("========================================")

        print("Dataset       :", dataset)
        print("Column        :", column_name)
        print("Rule          :", rule)

        # Apply validation rule
        invalid_df = apply_validation_rule(
            df,
            column_name,
            rule
        )

        # Count invalid records
        invalid_count = count_invalid_records(
            invalid_df
        )

        print(
            "Invalid Count :",
            invalid_count
        )

        # Store result
        results.append({
            "dataset": dataset,
            "column_name": column_name,
            "rule": rule,
            "invalid_count": invalid_count
        })

    return results


# ============================================================
# 11. INVALID CONDITION DISPATCHER
# ============================================================

def get_invalid_condition(
    column_name,
    rule
):

    if rule == "NOT_NULL":

        return not_null_invalid_condition(
            column_name
        )

    elif rule == "VALID_EMAIL":

        return email_invalid_condition(
            column_name
        )

    elif rule == "VALID_GENDER":

        return gender_invalid_condition(
            column_name
        )

    elif rule == "VALID_DOB":

        return dob_invalid_condition(
            column_name
        )

    else:

        raise ValueError(
            f"Unknown validation rule: {rule}"
        )


# ============================================================
# 12. BUILD COMBINED INVALID CONDITION
# ============================================================

def build_invalid_condition(rules_df):

    combined_condition = None

    for rule_row in rules_df.collect():

        column_name = rule_row["column_name"]
        rule = rule_row["rule"]

        print()
        print("Building condition for:")
        print("Column :", column_name)
        print("Rule   :", rule)

        current_condition = get_invalid_condition(
            column_name,
            rule
        )

        if combined_condition is None:

            combined_condition = current_condition

        else:

            combined_condition = (
                combined_condition
                | current_condition
            )

    return combined_condition


# ============================================================
# 13. SPLIT VALID AND INVALID RECORDS
# ============================================================

def split_valid_invalid_records(
    df,
    rules_df
):

    # Build one combined invalid condition
    invalid_condition = build_invalid_condition(
        rules_df
    )

    # Records that fail at least one rule
    invalid_df = (
        df.filter(
            invalid_condition
        )
    )

    # Records that pass all rules
    valid_df = (
        df.filter(
            ~invalid_condition
        )
    )

    return valid_df, invalid_df


# ============================================================
# 14. DISPLAY VALIDATION SUMMARY
# ============================================================

def display_validation_summary(
    validation_results
):

    print()
    print("========================================")
    print("DATA QUALITY VALIDATION SUMMARY")
    print("========================================")

    if not validation_results:

        print("No validation rules found.")

        return

    for result in validation_results:

        print()
        print(
            "Dataset       :",
            result["dataset"]
        )

        print(
            "Column        :",
            result["column_name"]
        )

        print(
            "Rule          :",
            result["rule"]
        )

        print(
            "Invalid Count :",
            result["invalid_count"]
        )

        print("----------------------------------------")


# ============================================================
# MAIN PROGRAM
# ============================================================

if __name__ == "__main__":

    # ========================================================
    # CREATE SPARK SESSION
    # ========================================================

    spark = (
        SparkSession.builder
        .appName("DataQualityTest")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    try:

        # ====================================================
        # LOAD VALIDATION RULES
        # ====================================================

        rules_path = (
            "data/Config/validation_rules.csv"
        )

        rules_df = load_validation_rules(
            spark,
            rules_path
        )


        # ====================================================
        # LOAD BRONZE CUSTOMER DATA
        # ====================================================

        customer_path = (
            "data/bronze/customers/customers.csv"
        )

        customers_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(customer_path)
        )


        # ====================================================
        # DISPLAY VALIDATION RULES
        # ====================================================

        print()
        print("===========================")
        print("VALIDATION RULES")
        print("===========================")

        rules_df.show(
            truncate=False
        )


        # ====================================================
        # DISPLAY RULE SCHEMA
        # ====================================================

        print()
        print("===========================")
        print("RULE SCHEMA")
        print("===========================")

        rules_df.printSchema()


        # ====================================================
        # DISPLAY CUSTOMER DATA SCHEMA
        # ====================================================

        print()
        print("===========================")
        print("CUSTOMER DATA SCHEMA")
        print("===========================")

        customers_df.printSchema()


        # ====================================================
        # DAY 13
        # APPLY ALL VALIDATION RULES
        # ====================================================

        validation_results = (
            apply_validation_rules(
                customers_df,
                rules_df
            )
        )


        # ====================================================
        # DISPLAY DAY 13 VALIDATION SUMMARY
        # ====================================================

        display_validation_summary(
            validation_results
        )


        # ====================================================
        # DAY 14 PART 2
        # SPLIT VALID AND INVALID RECORDS
        # ====================================================

        print()
        print("========================================")
        print("DAY 14 - VALID / INVALID SPLIT")
        print("========================================")

        valid_df, invalid_df = (
            split_valid_invalid_records(
                customers_df,
                rules_df
            )
        )


        # ====================================================
        # COUNT RECORDS
        # ====================================================

        total_count = customers_df.count()

        valid_count = valid_df.count()

        invalid_count = invalid_df.count()


        # ====================================================
        # DISPLAY RECORD COUNTS
        # ====================================================

        print()
        print("========================================")
        print("VALID / INVALID COUNTS")
        print("========================================")

        print(
            "Total Records  :",
            total_count
        )

        print(
            "Valid Records  :",
            valid_count
        )

        print(
            "Invalid Records:",
            invalid_count
        )


        # ====================================================
        # DISPLAY INVALID RECORDS
        # ====================================================

        print()
        print("========================================")
        print("INVALID RECORD SAMPLE")
        print("========================================")

        invalid_df.show(
            10,
            truncate=False
        )


        # ====================================================
        # DISPLAY VALID RECORDS
        # ========================================================

        print()
        print("========================================")
        print("VALID RECORD SAMPLE")
        print("========================================")

        valid_df.show(
            10,
            truncate=False
        )


        # ====================================================
        # RECONCILIATION CHECK
        # ========================================================

        print()
        print("========================================")
        print("RECONCILIATION CHECK")
        print("========================================")

        print(
            "Total Records       :",
            total_count
        )

        print(
            "Valid Records       :",
            valid_count
        )

        print(
            "Invalid Records     :",
            invalid_count
        )

        print(
            "Valid + Invalid     :",
            valid_count + invalid_count
        )


        if (
            total_count
            == valid_count + invalid_count
        ):

            print(
                "Reconciliation      : PASSED"
            )

        else:

            print(
                "Reconciliation      : FAILED"
            )


    finally:

        # ====================================================
        # STOP SPARK
        # ====================================================

        spark.stop()