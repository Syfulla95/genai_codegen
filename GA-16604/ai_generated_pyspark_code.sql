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
    ).join(final_data_df, "ProductGroup").withColumn(
        "ZScore_TotalValue", 
        (F.col("TotalValue") - F.col("mean_TotalValue")) / F.col("stddev_TotalValue")
    )

    # Step 11: Outlier Detection
    enhanced_final_data_df = standardized_df.withColumn("OutlierFlag", F.abs(F.col("ZScore_TotalValue")) > 2)

    # Write the enhanced final data to Unity Catalog target table
    spark.sql("DROP TABLE IF EXISTS catalog.target_db.enhanced_final_data")
    enhanced_final_data_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.enhanced_final_data")

    # Generate Outputs
    # Output 1: Summary Statistics by Region and Product Group
    summary_by_region_product_df = enhanced_final_data_df.groupBy("Region", "ProductGroup").agg(
        F.mean("TotalValue").alias("mean_TotalValue"),
        F.sum("TotalValue").alias("sum_TotalValue"),
        F.mean("Quantity").alias("mean_Quantity"),
        F.sum("Quantity").alias("sum_Quantity"),
        F.mean("Sales").alias("mean_Sales"),
        F.sum("Sales").alias("sum_Sales")
    )
    summary_by_region_product_df.show()

    # Output 2: Tenure Category Frequency by Region
    tenure_category_freq_df = enhanced_final_data_df.groupBy("TenureCategory", "Region").count()
    tenure_category_freq_df.show()

    # Output 3: Correlation Analysis
    correlation_df = enhanced_final_data_df.select("Sales", "Discount", "Quantity", "TotalValue").corr()
    logger.info("Correlation Analysis: %s", correlation_df)

    # Output 4: Summary by Outlier Flag
    summary_by_outlier_flag_df = enhanced_final_data_df.groupBy("OutlierFlag").agg(
        F.mean("Sales").alias("mean_Sales"),
        F.stddev("Sales").alias("stddev_Sales"),
        F.mean("TotalValue").alias("mean_TotalValue"),
        F.stddev("TotalValue").alias("stddev_TotalValue"),
        F.mean("Quantity").alias("mean_Quantity"),
        F.stddev("Quantity").alias("stddev_Quantity")
    )
    summary_by_outlier_flag_df.show()

except Exception as e:
    logger.error("An error occurred during the ETL process: %s", str(e))
    raise