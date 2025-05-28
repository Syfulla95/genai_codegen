import logging
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Load data from Unity Catalog tables
    new_customers_df = spark.table("catalog.source_db.new_customers")
    roi_new_customers_df = spark.table("catalog.source_db.roi_new_customers")
    uk_bank_holidays_df = spark.table("catalog.source_db.uk_bank_holidays")

    # Step 1: Remove unnecessary columns
    new_customers_df = new_customers_df.drop("Source Row Number")
    roi_new_customers_df = roi_new_customers_df.drop("Source Row Number", "Reporting Day")

    # Step 2: Change column types
    roi_new_customers_df = roi_new_customers_df.withColumn("Reporting Month", roi_new_customers_df["Reporting Month"].cast("string"))

    # Step 3: Join datasets
    joined_df = new_customers_df.join(roi_new_customers_df, "Reporting Date", "right")
    joined_df = joined_df.join(uk_bank_holidays_df, joined_df["Date"] == uk_bank_holidays_df["Date"], "right")

    # Step 4: Add calculated columns
    joined_df = joined_df.withColumn("Reporting Day", F.when(F.col("Day").startswith("S"), "N")
                                     .when(F.isnull(F.col("UK Bank Holiday")), "Y")
                                     .otherwise("N"))

    # Step 5: Remove unnecessary columns
    joined_df = joined_df.drop("Day", "UK Bank Holiday", "Reporting Day")

    # Step 6: Rename columns
    joined_df = joined_df.withColumnRenamed("Date", "Reporting Date")

    # Step 7: Aggregate data
    aggregated_df = joined_df.groupBy("Reporting Date").agg(F.sum("New Customers").alias("New Customers"))

    # Step 8: Add month column
    aggregated_df = aggregated_df.withColumn("Month", F.date_trunc("month", F.col("Reporting Date")))

    # Step 9: Calculate last day of the month
    window_spec = Window.partitionBy("Month")
    aggregated_df = aggregated_df.withColumn("Last Day", F.max("Reporting Date").over(window_spec))

    # Step 10: Add reporting month column
    aggregated_df = aggregated_df.withColumn("Reporting Month", F.when(F.col("Reporting Date") < F.col("Last Day"),
                                                                       F.concat(F.date_format(F.col("Reporting Date"), "MMMM"), F.lit("-"), F.year(F.col("Reporting Date"))))
                                               .otherwise(F.concat(F.date_format(F.date_add(F.col("Reporting Date"), 1), "MMMM"), F.lit("-"), F.year(F.date_add(F.col("Reporting Date"), 1)))))

    # Step 11: Remove last day column
    aggregated_df = aggregated_df.drop("Last Day")

    # Step 12: Filter out specific reporting month
    aggregated_df = aggregated_df.filter(F.col("Reporting Month") != "January-2024")

    # Step 13: Add reporting day using row number
    window_spec = Window.partitionBy("Reporting Month").orderBy("Reporting Date")
    aggregated_df = aggregated_df.withColumn("Reporting Day", F.row_number().over(window_spec))

    # Step 14: Rename fields with prefix "ROI"
    aggregated_df = aggregated_df.withColumnRenamed("Reporting Date", "ROI Reporting Date") \
                                 .withColumnRenamed("Reporting Month", "ROI Reporting Month") \
                                 .withColumnRenamed("New Customers", "ROI New Customers") \
                                 .withColumnRenamed("Reporting Day", "ROI Reporting Day")

    # Step 15: Aggregate data by reporting month, day, and date
    final_df = aggregated_df.groupBy("ROI Reporting Month", "ROI Reporting Day", "ROI Reporting Date") \
                            .agg(F.sum("ROI New Customers").alias("ROI New Customers"),
                                 F.max("ROI Reporting Month").alias("ROI Reporting Month"))

    # Step 16: Add misalignment flag
    final_df = final_df.withColumn("Misalignment Flag", F.when(F.substring(F.col("Reporting Month"), 1, 3) != F.substring(F.col("ROI Reporting Month"), 1, 3), "X")
                                  .when(F.isnull(F.col("ROI Reporting Month")), "X")
                                  .otherwise(None))

    # Step 17: Rename column "New Customers" to "UK New Customers"
    final_df = final_df.withColumnRenamed("ROI New Customers", "UK New Customers")

    # Step 18: Write to Unity Catalog target table
    spark.sql("DROP TABLE IF EXISTS catalog.target_db.new_customers_ready_to_report")
    final_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.new_customers_ready_to_report")

    logger.info("Data transformation and loading completed successfully.")

except Exception as e:
    logger.error(f"An error occurred during the ETL process: {e}")