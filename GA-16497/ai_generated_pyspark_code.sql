import logging
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType
import psycopg2
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Assume Spark session is already initialized as 'spark'
# Load data from Unity Catalog tables
try:
    historical_sales_df = spark.table("catalog.db.historical_sales")
    hospital_stats_df = spark.table("catalog.db.hospital_stats")
    logistics_channels_df = spark.table("catalog.db.logistics_channels")
    compensation_guidelines_df = spark.table("catalog.db.compensation_guidelines")
    employment_details_df = spark.table("catalog.db.employment_details")
    company_goals_df = spark.table("catalog.db.company_goals")
    third_party_sales_trends_df = spark.table("catalog.db.third_party_sales_trends")
    hospital_sales_assignments_df = spark.table("catalog.db.hospital_sales_assignments")
    growth_opportunities_df = spark.table("catalog.db.growth_opportunities")
except Exception as e:
    logger.error(f"Error loading data from Unity Catalog: {e}")
    raise

# Transformation Logic
try:
    # Join operations
    joined_df = historical_sales_df.join(hospital_stats_df, "Hospital_ID", "left") \
                                   .join(logistics_channels_df, "Channel_ID", "left") \
                                   .join(compensation_guidelines_df, "Associate_ID", "left") \
                                   .join(employment_details_df, "Associate_ID", "left") \
                                   .join(company_goals_df, "Year", "left") \
                                   .join(third_party_sales_trends_df, "Channel_Type", "left") \
                                   .join(hospital_sales_assignments_df, "Hospital_ID", "left") \
                                   .join(growth_opportunities_df, "Channel_ID", "left")

    # Select and rename fields
    selected_df = joined_df.select(
        F.col("Year").alias("Year"),
        F.col("Channel_ID").alias("Channel_ID"),
        F.col("Hospital_ID").alias("Hospital_ID"),
        F.col("Growth_Target").alias("Growth_Target"),
        F.col("Investment_Planned").alias("Investment_Planned"),
        F.col("Market_Trend").alias("Market_Trend"),
        F.col("Political_Impact").alias("Political_Impact"),
        F.col("Economic_Impact").alias("Economic_Impact"),
        F.col("Director_Name").alias("Director_Name"),
        F.col("Manager_Name").alias("Manager_Name"),
        F.col("Market_Potential").alias("Market_Potential"),
        F.col("Projected_Growth_Rate").alias("Projected_Growth_Rate"),
        F.col("Investment_Required").alias("Investment_Required"),
        F.col("Expected_ROI").alias("Expected_ROI")
    )

    # Custom Calculations
    compensation_df = selected_df.withColumn(
        "Compensation",
        F.col("Base_Salary") + (F.col("Commission_Percentage") * F.col("Base_Salary")) + F.col("Bonus")
    )

    # Generate Rows for Target Year
    target_year_df = compensation_df.withColumn("Target Year", F.lit(2023))
    target_year_df = target_year_df.withColumn("Target Year", F.expr("sequence(Target Year, 2026)"))

    # Projected Sales Growth Rate Calculation
    projected_sales_growth_rate_df = target_year_df.withColumn(
        "Projected_Sales_Growth_Rate",
        F.when(F.col("Target Year") == 2024, F.col("Projected_Growth_Rate") + (F.col("Projected_Growth_Rate") / 100))
         .when(F.col("Target Year") == 2025, (F.col("Projected_Growth_Rate") + (F.col("Projected_Growth_Rate") / 100)) + (F.col("Projected_Growth_Rate") / 100))
         .when(F.col("Target Year") == 2026, ((F.col("Projected_Growth_Rate") + (F.col("Projected_Growth_Rate") / 100)) + (F.col("Projected_Growth_Rate") / 100)) + (F.col("Projected_Growth_Rate") / 100))
         .otherwise(F.col("Projected_Growth_Rate"))
    )

    # Projected Investments Calculation
    projected_investments_df = projected_sales_growth_rate_df.withColumn(
        "projected_investments",
        F.when(F.col("Target Year") == 2024, F.col("Investment_Planned") * (F.col("Projected_Sales_Growth_Rate") / 100))
         .when(F.col("Target Year") == 2025, F.col("Investment_Planned") * (1 + F.col("Projected_Sales_Growth_Rate") / 100))
         .when(F.col("Target Year") == 2026, F.col("Investment_Planned") * (1 + F.col("Projected_Sales_Growth_Rate") / 100))
         .otherwise(F.col("Investment_Planned"))
    )

    # Projected Revenue Calculation
    projected_revenue_df = projected_investments_df.withColumn(
        "Projected Revenue",
        F.when(F.col("Target Year") == 2024, F.col("Sales_Revenue") * (F.col("Projected_Sales_Growth_Rate") / 100))
         .when(F.col("Target Year") == 2025, F.col("Sales_Revenue") * (1 + F.col("Projected_Sales_Growth_Rate") / 100))
         .when(F.col("Target Year") == 2026, F.col("Sales_Revenue") * (1 + F.col("Projected_Sales_Growth_Rate") / 100))
         .otherwise(F.col("Sales_Revenue"))
    )

    # Filter Condition
    filtered_df = projected_revenue_df.filter(F.col("Target Year") > 2023)

    # BIA_SHIP_HNDL_AMT Calculation
    bia_ship_hndl_amt_df = filtered_df.withColumn(
        "BIA_SHIP_HNDL_AMT",
        F.col("Sum_Trans_Charge_Amt") + F.col("Sum_RESTOCK_Fee") + F.col("Sum_Special_Hndl_Amt") +
        F.col("Sum_Vendor_Hndl_Amt") + F.col("Sum_MOC_Amt") + F.col("Sum_Fuel_Surcharge")
    )

    # COE_SHIP_HNDL_AMT Calculation
    coe_ship_hndl_amt_df = bia_ship_hndl_amt_df.withColumn(
        "COE_SHIP_HNDL_AMT",
        F.col("Sum_Trans_Charge_Amt") + F.col("Sum_RESTOCK_Fee") + F.col("Sum_Special_Hndl_Amt") +
        F.col("Sum_Vendor_Hndl_Amt") + F.col("Sum_MOC_Amt") + F.col("Sum_Fuel_Surcharge") +
        F.col("Rush_Order_Fee") + F.col("VENDR_TRANS_CHRG_FRT_ZTV1") + F.col("MARKUP_VENDOR_TRANS_FEE_AMT_ZMT1")
    )

    # Invoice_Sales Calculation
    invoice_sales_df = coe_ship_hndl_amt_df.withColumn(
        "Invoice_Sales",
        F.col("Sum_EXT_FINAL_PRICE")
    )

    # Null Handling
    null_handling_df = invoice_sales_df.withColumn(
        "null_yn",
        F.when(F.col("FNC_ID").isNull() | F.col("FNC_ID") == "", "Y")
         .when(F.col("Whs").isNull() | F.col("Whs") == "", "Y")
         .when(F.col("DIST_CHNL_ID").isNull() | F.col("DIST_CHNL_ID") == "", "Y")
         .when(F.col("SOLDTO").isNull() | F.col("SOLDTO") == "", "Y")
         .when(F.col("SHIPTO").isNull() | F.col("SHIPTO") == "", "Y")
         .otherwise("N")
    )

    # Filter Calculation
    filtered_null_df = null_handling_df.filter(F.col("null_yn") == "Y")

    # Update Null Values
    updated_null_df = filtered_null_df.withColumn(
        "FNC_ID",
        F.when(F.col("FNC_ID").isNull() | F.col("FNC_ID") == "", "OTH").otherwise(F.col("FNC_ID"))
    ).withColumn(
        "FNC_DESC",
        F.when(F.col("FNC_DESC").isNull() | F.col("FNC_DESC") == "", "OTHER").otherwise(F.col("FNC_DESC"))
    )

    # Final Union
    final_df = updated_null_df.union(filtered_null_df)

    # Output Handling
    # Drop existing table if necessary
    spark.sql("DROP TABLE IF EXISTS catalog.target_db.target_table")

    # Write to Unity Catalog target table
    final_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.target_table")

except Exception as e:
    logger.error(f"Error during transformation or output handling: {e}")
    raise

logger.info("ETL process completed successfully.")