import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    year,
    current_date,
    sum,
    avg,
    count,
    when
)

from delta import configure_spark_with_delta_pip

# -------------------------
# LOGGING
# -------------------------

logging.basicConfig(
    filename='logs/pipeline.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

logging.info("TRS Pension Pipeline Started")

# -------------------------
# SPARK SESSION
# -------------------------

builder = SparkSession.builder \
    .appName("TRSPensionPlatform") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

try:

    # -------------------------
    # BRONZE LAYER
    # -------------------------

    members = spark.read.csv(
        "data/bronze/members.csv",
        header=True,
        inferSchema=True
    )

    historical_contributions = spark.read.csv(
    "data/bronze/contributions.csv",
    header=True,
    inferSchema=True
	)

    new_contributions = spark.read.csv(
    "data/bronze/new_contributions_march.csv",
    header=True,
    inferSchema=True
	)

    contributions = historical_contributions.union(new_contributions)

    pension_payments = spark.read.csv(
        "data/bronze/pension_payments.csv",
        header=True,
        inferSchema=True
    )

    healthcare_claims = spark.read.csv(
        "data/bronze/healthcare_claims.csv",
        header=True,
        inferSchema=True
    )

    logging.info("Bronze layer loaded successfully")

    # -------------------------
    # SILVER LAYER
    # -------------------------

    members_clean = members.withColumn(
        "retirement_eligible",
        when(
            (members.age >= 60) &
            (members.years_of_service >= 20),
            "Yes"
        ).otherwise("No")
    )

    contribution_summary = contributions.groupBy("member_id").agg(
        sum("employee_contribution").alias("total_employee_contributions"),
        sum("employer_contribution").alias("total_employer_contributions")
    )

    pension_summary = pension_payments.groupBy("member_id").agg(
        sum("monthly_pension").alias("total_pension_paid")
    )

    healthcare_summary = healthcare_claims.groupBy("member_id").agg(
        sum("claim_amount").alias("total_healthcare_claims")
    )

    silver_df = members_clean \
        .join(contribution_summary, on="member_id", how="left") \
        .join(pension_summary, on="member_id", how="left") \
        .join(healthcare_summary, on="member_id", how="left") \
        .fillna(0)

    logging.info("Silver layer transformations completed")

    silver_df.write.format("delta").mode("overwrite").save(
        "data/silver/member_benefits"
    )

    # -------------------------
    # GOLD LAYER
    # -------------------------

    gold_df = silver_df.groupBy(
        "employment_status"
    ).agg(
        count("*").alias("total_members"),
        avg("salary").alias("average_salary"),
        sum("total_employee_contributions").alias("employee_contributions"),
        sum("total_employer_contributions").alias("employer_contributions"),
        sum("total_pension_paid").alias("pension_payouts"),
        sum("total_healthcare_claims").alias("healthcare_claims")
    )

    logging.info("Gold layer aggregations completed")

    gold_df.show()

    gold_df.toPandas().to_csv(
        "data/gold/trs_pension_dashboard.csv",
        index=False
    )

    logging.info("Gold export completed")

    logging.info("TRS Pension Pipeline Completed Successfully")

except Exception as e:
    logging.error(f"Pipeline failed: {str(e)}")
    print(f"Pipeline failed: {str(e)}")
