from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    trim,
    lower,
    upper,
    to_date,
    to_timestamp,
    current_date,
    lit,
    when
)

# ============================================================
# IMPORT DATA QUALITY FRAMEWORK
# ============================================================

from data_quality import (
    load_validation_rules,
    split_valid_invalid_records
)


# ============================================================
# MAIN PROGRAM
# ============================================================

if __name__ == "__main__":

    # ========================================================
    # 1. CREATE SPARK SESSION
    # ========================================================

    spark = (
        SparkSession.builder
        .appName("BronzeToSilverCustomers")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    try:

        # ====================================================
        # 2. BRONZE CUSTOMER DATA PATH
        # ====================================================

        customer_path = (
            "data/bronze/customers/customers.csv"
        )


        # ====================================================
        # 3. VALIDATION RULES PATH
        # ====================================================

        rules_path = (
            "data/Config/validation_rules.csv"
        )


        # ====================================================
        # 4. READ BRONZE CUSTOMER DATA
        # ====================================================

        print()
        print("========================================")
        print("READING BRONZE CUSTOMER DATA")
        print("========================================")

        customers_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(customer_path)
        )


        # ====================================================
        # 5. LOAD VALIDATION RULES
        # ====================================================

        print()
        print("========================================")
        print("LOADING VALIDATION RULES")
        print("========================================")

        rules_df = load_validation_rules(
            spark,
            rules_path
        )

        rules_df.show(
            truncate=False
        )


        # ====================================================
        # 6. BRONZE RECORD COUNT
        # ====================================================

        total_count = customers_df.count()

        print()
        print(
            "Bronze records :",
            total_count
        )


        # ====================================================
        # 7. DISPLAY BRONZE DATA
        # ====================================================

        print()
        print("========================================")
        print("BRONZE CUSTOMER SAMPLE")
        print("========================================")

        customers_df.show(
            10,
            truncate=False
        )


        # ====================================================
        # 8. CLEAN / STANDARDIZE DATA
        # ====================================================

        print()
        print("========================================")
        print("CLEANING / STANDARDIZATION")
        print("========================================")

        cleaned_df = (

            customers_df

            # ------------------------------------------------
            # CUSTOMER ID
            # ------------------------------------------------

            .withColumn(
                "customer_id",
                trim(
                    col("customer_id")
                )
            )

            # ------------------------------------------------
            # FIRST NAME
            # ------------------------------------------------

            .withColumn(
                "first_name",
                trim(
                    col("first_name")
                )
            )

            # ------------------------------------------------
            # LAST NAME
            # ------------------------------------------------

            .withColumn(
                "last_name",
                trim(
                    col("last_name")
                )
            )

            # ------------------------------------------------
            # EMAIL
            # ------------------------------------------------

            .withColumn(
                "email",
                lower(
                    trim(
                        col("email")
                    )
                )
            )

            # ------------------------------------------------
            # GENDER
            # ------------------------------------------------

            .withColumn(
                "gender",
                upper(
                    trim(
                        col("gender")
                    )
                )
            )

            # ------------------------------------------------
            # DATE OF BIRTH
            # ------------------------------------------------

            .withColumn(
                "date_of_birth",
                to_date(
                    col("date_of_birth")
                )
            )

            # ------------------------------------------------
            # PHONE
            # ------------------------------------------------

            .withColumn(
                "phone",
                trim(
                    col("phone").cast("string")
                )
            )

            # ------------------------------------------------
            # CITY
            # ------------------------------------------------

            .withColumn(
                "city",
                trim(
                    col("city")
                )
            )

            # ------------------------------------------------
            # CREATED AT
            # ------------------------------------------------

            .withColumn(
                "created_at",
                to_timestamp(
                    col("created_at")
                )
            )

            # ------------------------------------------------
            # MODIFIED AT
            # ------------------------------------------------

            .withColumn(
                "modified_at",
                to_timestamp(
                    col("modified_at")
                )
            )
        )


        # ====================================================
        # 9. COUNT CLEANED RECORDS
        # ====================================================

        cleaned_count = cleaned_df.count()

        print()
        print(
            "Cleaned records :",
            cleaned_count
        )


        # ====================================================
        # 10. DISPLAY CLEANED DATA
        # ====================================================

        print()
        print("========================================")
        print("CLEANED CUSTOMER SAMPLE")
        print("========================================")

        cleaned_df.show(
            10,
            truncate=False
        )


        # ====================================================
        # 11. APPLY CONFIGURATION-DRIVEN DATA QUALITY
        # ====================================================

        print()
        print("========================================")
        print("CONFIGURATION-DRIVEN DATA QUALITY")
        print("========================================")

        valid_df, invalid_df = (
            split_valid_invalid_records(
                cleaned_df,
                rules_df
            )
        )


        # ====================================================
        # 12. DISPLAY VALID RECORDS
        # ====================================================

        print()
        print("========================================")
        print("VALID CUSTOMER SAMPLE")
        print("========================================")

        valid_df.show(
            10,
            truncate=False
        )


        # ====================================================
        # 13. ADD QUARANTINE REASON
        # ====================================================

        invalid_df = (

            invalid_df

            .withColumn(
                "validation_reason",

                when(
                    col("email").isNull()
                    | (
                        trim(
                            col("email")
                        ) == ""
                    ),
                    lit("MISSING_EMAIL")
                )

                .when(
                    col("email").isNotNull()
                    & (
                        ~col("email").rlike(
                            r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
                        )
                    ),
                    lit("INVALID_EMAIL")
                )

                .when(
                    col("gender").isNotNull()
                    & (
                        ~col("gender").isin(
                            "M",
                            "F"
                        )
                    ),
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


        # ====================================================
        # 14. DISPLAY INVALID RECORDS
        # ====================================================

        print()
        print("========================================")
        print("INVALID CUSTOMER SAMPLE")
        print("========================================")

        invalid_df.show(
            10,
            truncate=False
        )


        # ====================================================
        # 15. COUNT VALID / INVALID RECORDS
        # ====================================================

        valid_count = valid_df.count()

        invalid_count = invalid_df.count()


        # ====================================================
        # 16. PROCESSING SUMMARY
        # ====================================================

        print()
        print("========================================")
        print("CUSTOMER PROCESSING SUMMARY")
        print("========================================")

        print(
            "Bronze records      :",
            total_count
        )

        print(
            "Cleaned records     :",
            cleaned_count
        )

        print(
            "Valid records       :",
            valid_count
        )

        print(
            "Invalid records     :",
            invalid_count
        )

        print(
            "Valid + Invalid     :",
            valid_count + invalid_count
        )


        # ====================================================
        # 17. RECONCILIATION CHECK
        # ====================================================

        print()
        print("========================================")
        print("RECONCILIATION CHECK")
        print("========================================")

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


        # ====================================================
        # 18. WRITE VALID DATA TO SILVER
        # ====================================================

        silver_path = (
            "data/silver/customers"
        )

        print()
        print("========================================")
        print("WRITING SILVER DATA")
        print("========================================")

        (
            valid_df.write
            .mode("overwrite")
            .parquet(silver_path)
        )

        print(
            "Silver output       :",
            silver_path
        )


        # ====================================================
        # 19. WRITE INVALID DATA TO QUARANTINE
        # ====================================================

        quarantine_path = (
            "data/quarantine/customers"
        )

        print()
        print("========================================")
        print("WRITING QUARANTINE DATA")
        print("========================================")

        (
            invalid_df.write
            .mode("overwrite")
            .parquet(quarantine_path)
        )

        print(
            "Quarantine output   :",
            quarantine_path
        )


        # ====================================================
        # 20. FINAL PIPELINE SUMMARY
        # ====================================================

        print()
        print("========================================")
        print("PIPELINE COMPLETED")
        print("========================================")

        print(
            "Bronze records      :",
            total_count
        )

        print(
            "Silver records      :",
            valid_count
        )

        print(
            "Quarantine records  :",
            invalid_count
        )

        print(
            "Silver + Quarantine:",
            valid_count + invalid_count
        )

        print(
            "Reconciliation      :",
            "PASSED"
            if total_count == valid_count + invalid_count
            else "FAILED"
        )

        print()
        print(
            "Silver path         :",
            silver_path
        )

        print(
            "Quarantine path     :",
            quarantine_path
        )


    finally:

        # ====================================================
        # 21. STOP SPARK
        # ====================================================

        spark.stop()