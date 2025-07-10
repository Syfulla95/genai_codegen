import logging
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DateType
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Load data from Unity Catalog tables
    customers_df = spark.table("catalog.source_db.customers")
    transactions_df = spark.table("catalog.source_db.transactions")

    # Step 1: Valid Transactions Filtering
    valid_txns_df = transactions_df.filter((F.col("Sales") > 0) & (F.col("Product") != ""))

    # Step 2: Effective Price Calculation
    trans_step2_df = valid_txns_df.withColumn("EffectivePrice", F.col("Sales") * (1 - F.col("Discount") / 100))

    # Step 3: Total Value Calculation
    trans_step3_df = trans_step2_df.withColumn("TotalValue", F.col("EffectivePrice") * F.col("Quantity"))

    # Step 4: Joining with Customers Data
    full_data_df = trans_step3_df.join(customers_df, trans_step3_df.CustomerID == customers_df.CustomerID, "left") \
                                 .select(trans_step3_df["*"], customers_df["Region"], customers_df["JoinDate"])

    # Step 5: Tenure Days Calculation
    trans_step5_df = full_data_df.withColumn("TenureDays", F.datediff(F.col("TransDate"), F.col("JoinDate")))

    # Step 6: Tenure Category Assignment
    trans_step6_df = trans_step5_df.withColumn("TenureCategory", 
                                               F.when(F.col("TenureDays") < 180, "New")
                                                .when(F.col("TenureDays") < 365, "Medium")
                                                .otherwise("Loyal"))

    # Step 7: High Value Flag
    trans_step7_df = trans_step6_df.withColumn("HighValueFlag", F.col("TotalValue") > 2000)

    # Step 8: Product Group Assignment
    trans_step8_df = trans_step7_df.withColumn("ProductGroup", 
                                               F.when(F.col("Product").isin(["A", "C"]), "Core")
                                                .otherwise("Non-Core"))

    # Step 9: Z-score Standardization
    standardized_df = trans_step8_df.groupBy("ProductGroup").agg(
        F.mean("TotalValue").alias("mean"),
        F.stddev("TotalValue").alias("stddev")
    ).join(trans_step8_df, "ProductGroup").withColumn(
        "ZScoreTotalValue", (F.col("TotalValue") - F.col("mean")) / F.col("stddev")
    )

    # Step 10: Outlier Detection
    enhanced_final_data_df = standardized_df.withColumn("OutlierFlag", F.abs(F.col("ZScoreTotalValue")) > 2)

    # Write final data to Unity Catalog table
    spark.sql("DROP TABLE IF EXISTS catalog.target_db.enhanced_final_data")
    enhanced_final_data_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.enhanced_final_data")

    # Generate reports
    # Correlation Analysis Report
    correlation_df = enhanced_final_data_df.select("Sales", "Discount", "Quantity", "TotalValue")
    correlation_df.stat.corr("Sales", "Discount")
    correlation_df.stat.corr("Sales", "Quantity")
    correlation_df.stat.corr("Sales", "TotalValue")
    correlation_df.stat.corr("Discount", "Quantity")
    correlation_df.stat.corr("Discount", "TotalValue")
    correlation_df.stat.corr("Quantity", "TotalValue")

    # Tenure Category Frequency Report
    tenure_freq_df = enhanced_final_data_df.groupBy("TenureCategory", "Region").count()

    # Outlier Summary Report
    outlier_summary_df = enhanced_final_data_df.groupBy("OutlierFlag").agg(
        F.mean("Sales").alias("mean_sales"),
        F.mean("TotalValue").alias("mean_total_value"),
        F.mean("Quantity").alias("mean_quantity"),
        F.stddev("Sales").alias("stddev_sales"),
        F.stddev("TotalValue").alias("stddev_total_value"),
        F.stddev("Quantity").alias("stddev_quantity")
    )

    # Summary Statistics Report
    summary_stats_df = enhanced_final_data_df.groupBy("Region", "ProductGroup").agg(
        F.mean("TotalValue").alias("mean_total_value"),
        F.sum("TotalValue").alias("sum_total_value"),
        F.mean("Quantity").alias("mean_quantity"),
        F.sum("Quantity").alias("sum_quantity"),
        F.mean("Sales").alias("mean_sales"),
        F.sum("Sales").alias("sum_sales")
    )

    # Save reports to Unity Catalog tables
    spark.sql("DROP TABLE IF EXISTS catalog.target_db.correlation_report")
    correlation_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.correlation_report")

    spark.sql("DROP TABLE IF EXISTS catalog.target_db.tenure_category_frequency_report")
    tenure_freq_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.tenure_category_frequency_report")

    spark.sql("DROP TABLE IF EXISTS catalog.target_db.outlier_summary_report")
    outlier_summary_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.outlier_summary_report")

    spark.sql("DROP TABLE IF EXISTS catalog.target_db.summary_statistics_report")
    summary_stats_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.summary_statistics_report")

    logger.info("ETL process completed successfully.")

except Exception as e:
    logger.error(f"An error occurred during the ETL process: {e}")