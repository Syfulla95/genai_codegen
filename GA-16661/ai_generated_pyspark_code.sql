import logging
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DateType
from datetime import datetime

# Set up logging
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

    # Step 4: Full Data Join
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

    # Step 9: Final Data Preparation
    final_data_df = trans_step8_df

    # Step 10: Z-score Standardization
    standardized_df = final_data_df.groupBy("ProductGroup").agg(
        F.mean("TotalValue").alias("mean_TotalValue"),
        F.stddev("TotalValue").alias("stddev_TotalValue")
    ).join(final_data_df, "ProductGroup") \
     .withColumn("ZScore_TotalValue", (F.col("TotalValue") - F.col("mean_TotalValue")) / F.col("stddev_TotalValue"))

    # Step 11: Outlier Detection
    enhanced_final_data_df = standardized_df.withColumn("OutlierFlag", F.abs(F.col("ZScore_TotalValue")) > 2)

    # Write the enhanced final data to Unity Catalog target table
    spark.sql("DROP TABLE IF EXISTS catalog.target_db.enhanced_final_data")
    enhanced_final_data_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.enhanced_final_data")

    # Generate summary statistics by Region and Product Group
    summary_stats_df = enhanced_final_data_df.groupBy("Region", "ProductGroup").agg(
        F.mean("TotalValue").alias("TotalValue_Mean"),
        F.sum("TotalValue").alias("TotalValue_Sum"),
        F.mean("Quantity").alias("Quantity_Mean"),
        F.sum("Quantity").alias("Quantity_Sum"),
        F.mean("Sales").alias("Sales_Mean"),
        F.sum("Sales").alias("Sales_Sum")
    )

    # Write summary statistics to Unity Catalog target table
    spark.sql("DROP TABLE IF EXISTS catalog.target_db.summary_stats")
    summary_stats_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.summary_stats")

    # Generate frequency of Tenure Category by Region
    tenure_freq_df = enhanced_final_data_df.groupBy("TenureCategory", "Region").count()

    # Write tenure frequency to Unity Catalog target table
    spark.sql("DROP TABLE IF EXISTS catalog.target_db.tenure_freq")
    tenure_freq_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.tenure_freq")

    # Perform correlation analysis
    correlation_df = enhanced_final_data_df.select("Sales", "Discount", "Quantity", "TotalValue").corr()

    # Write correlation analysis to Unity Catalog target table
    spark.sql("DROP TABLE IF EXISTS catalog.target_db.correlation_analysis")
    correlation_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.correlation_analysis")

    # Generate summary by Outlier Flag
    outlier_summary_df = enhanced_final_data_df.groupBy("OutlierFlag").agg(
        F.mean("Sales").alias("Sales_Mean"),
        F.stddev("Sales").alias("Sales_StdDev"),
        F.mean("TotalValue").alias("TotalValue_Mean"),
        F.stddev("TotalValue").alias("TotalValue_StdDev"),
        F.mean("Quantity").alias("Quantity_Mean"),
        F.stddev("Quantity").alias("Quantity_StdDev")
    )

    # Write outlier summary to Unity Catalog target table
    spark.sql("DROP TABLE IF EXISTS catalog.target_db.outlier_summary")
    outlier_summary_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.outlier_summary")

    logger.info("ETL process completed successfully.")

except Exception as e:
    logger.error(f"An error occurred during the ETL process: {e}")
    raise