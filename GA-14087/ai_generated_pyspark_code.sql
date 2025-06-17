import logging
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Load data from Unity Catalog tables
    associates_df = spark.table("genai_demo.cardinal_health.sales_associates_employment_details")
    compensation_df = spark.table("genai_demo.cardinal_health.compensation_guidelines")
    hospital_stats_df = spark.table("genai_demo.cardinal_health.hospital_stats_north_america")
    hospital_assignments_df = spark.table("genai_demo.cardinal_health.hospital_sales_assignments")
    logistics_channels_df = spark.table("genai_demo.cardinal_health.logistics_channels")
    growth_opportunities_df = spark.table("genai_demo.cardinal_health.growth_opportunities")
    third_party_trends_df = spark.table("genai_demo.cardinal_health.third_party_sales_trends")
    historical_sales_df = spark.table("genai_demo.cardinal_health.historical_sales")
    company_goals_df = spark.table("genai_demo.cardinal_health.company_goals")

    # Transformation: Join associates with compensation
    associates_compensation_df = associates_df.join(
        compensation_df,
        associates_df.Associate_ID == compensation_df.Associate_ID,
        "inner"
    ).select(
        associates_df.Associate_ID,
        associates_df.Associate_Name,
        associates_df.Region,
        associates_df.Employment_Type,
        associates_df.Years_of_Experience,
        associates_df.Department,
        compensation_df.Base_Salary,
        compensation_df.Commission_Percentage,
        compensation_df.Bonus
    )

    # Transformation: Calculate total compensation
    associates_compensation_df = associates_compensation_df.withColumn(
        "Compensation",
        F.col("Base_Salary") + (F.col("Commission_Percentage") / 100 * F.col("Base_Salary")) + F.col("Bonus")
    )

    # Transformation: Join hospital stats with assignments
    hospital_assignments_df = hospital_stats_df.join(
        hospital_assignments_df,
        (hospital_stats_df.Hospital_ID == hospital_assignments_df.Hospital_ID) &
        (hospital_stats_df.Hospital_Name == hospital_assignments_df.Hospital_Name),
        "inner"
    ).select(
        hospital_stats_df.Hospital_ID,
        hospital_stats_df.Hospital_Name,
        hospital_stats_df.Number_of_Beds,
        hospital_stats_df.Annual_Revenue,
        hospital_stats_df.Patient_Satisfaction_Score,
        hospital_assignments_df.Director_Name,
        hospital_assignments_df.Manager_Name,
        hospital_assignments_df.Associate_ID,
        hospital_assignments_df.Associate_Name
    )

    # Transformation: Join associates_compensation with hospital_assignments
    associates_hospital_df = associates_compensation_df.join(
        hospital_assignments_df,
        (associates_compensation_df.Associate_ID == hospital_assignments_df.Associate_ID) &
        (associates_compensation_df.Associate_Name == hospital_assignments_df.Associate_Name),
        "inner"
    ).select(
        associates_compensation_df.Associate_ID,
        associates_compensation_df.Associate_Name,
        associates_compensation_df.Compensation,
        hospital_assignments_df.Hospital_ID,
        hospital_assignments_df.Hospital_Name,
        hospital_assignments_df.Director_Name,
        hospital_assignments_df.Manager_Name
    )

    # Transformation: Join logistics_channels with growth_opportunities
    logistics_growth_df = logistics_channels_df.join(
        growth_opportunities_df,
        (logistics_channels_df.Channel_ID == growth_opportunities_df.Channel_ID) &
        (logistics_channels_df.Channel_Type == growth_opportunities_df.Channel_Type),
        "inner"
    ).select(
        logistics_channels_df.Channel_ID,
        logistics_channels_df.Channel_Type,
        logistics_channels_df.Hospital_ID,
        logistics_channels_df.Growth_Opportunities,
        growth_opportunities_df.Projected_Growth_Rate,
        growth_opportunities_df.Market_Potential,
        growth_opportunities_df.Investment_Required,
        growth_opportunities_df.Expected_ROI
    ).dropDuplicates(["Channel_ID", "Channel_Type", "Hospital_ID"])

    # Transformation: Join associates_hospital with logistics_growth
    final_df = associates_hospital_df.join(
        logistics_growth_df,
        associates_hospital_df.Hospital_ID == logistics_growth_df.Hospital_ID,
        "inner"
    ).select(
        associates_hospital_df.Hospital_ID,
        associates_hospital_df.Compensation,
        logistics_growth_df.Channel_ID,
        logistics_growth_df.Channel_Type,
        logistics_growth_df.Growth_Opportunities,
        logistics_growth_df.Projected_Growth_Rate,
        logistics_growth_df.Market_Potential,
        logistics_growth_df.Investment_Required,
        logistics_growth_df.Expected_ROI
    )

    # Transformation: Join historical_sales with third_party_trends
    sales_trends_df = historical_sales_df.join(
        third_party_trends_df,
        historical_sales_df.Channel_Type == third_party_trends_df.Channel_Type,
        "inner"
    ).select(
        historical_sales_df.Year,
        historical_sales_df.Channel_ID,
        historical_sales_df.Channel_Type,
        historical_sales_df.Sales_Revenue,
        historical_sales_df.Hospital_ID,
        third_party_trends_df.Market_Trend,
        third_party_trends_df.Political_Impact,
        third_party_trends_df.Economic_Impact
    ).dropDuplicates(["Year", "Channel_Type", "Sales_Revenue"])

    # Transformation: Join final_df with sales_trends_df
    final_sales_df = final_df.join(
        sales_trends_df,
        final_df.Hospital_ID == sales_trends_df.Hospital_ID,
        "inner"
    ).select(
        final_df.Hospital_ID,
        final_df.Channel_Type,
        final_df.Projected_Growth_Rate,
        sales_trends_df.Year,
        sales_trends_df.Channel_ID,
        sales_trends_df.Channel_Type,
        sales_trends_df.Sales_Revenue
    )

    # Transformation: Generate rows for Target Year from 2023 to 2026
    target_years_df = spark.range(2023, 2027).withColumnRenamed("id", "Target Year")

    # Transformation: Calculate Projected Sales Growth Rate
    final_sales_df = final_sales_df.crossJoin(target_years_df).withColumn(
        "Projected_Sales_Growth_Rate",
        F.when(F.col("Target Year") == 2024, F.col("Projected_Growth_Rate") + (F.col("Projected_Growth_Rate") / 100))
        .when(F.col("Target Year") == 2025, (F.col("Projected_Growth_Rate") + (F.col("Projected_Growth_Rate") / 100)) + (F.col("Projected_Growth_Rate") / 100))
        .when(F.col("Target Year") == 2026, ((F.col("Projected_Growth_Rate") + (F.col("Projected_Growth_Rate") / 100)) + (F.col("Projected_Growth_Rate") / 100)) + (F.col("Projected_Growth_Rate") / 100))
        .otherwise(F.col("Projected_Growth_Rate"))
    )

    # Transformation: Calculate Projected Revenue
    final_sales_df = final_sales_df.withColumn(
        "Projected Revenue",
        F.when(F.col("Target Year") == 2024, F.col("Sales_Revenue") * (F.col("Projected_Sales_Growth_Rate") / 100))
        .when(F.col("Target Year") == 2025, F.col("Sales_Revenue") * (1 + F.col("Projected_Sales_Growth_Rate") / 100))
        .when(F.col("Target Year") == 2026, F.col("Sales_Revenue") * (1 + F.col("Projected_Sales_Growth_Rate") / 100))
        .otherwise(F.col("Sales_Revenue"))
    )

    # Transformation: Filter records where Target Year is greater than 2023
    final_sales_df = final_sales_df.filter(F.col("Target Year") > 2023)

    # Transformation: Sort records by Target Year in ascending order
    final_sales_df = final_sales_df.orderBy("Target Year")

    # Write the final DataFrame to Unity Catalog table
    final_sales_df.write.format("delta").mode("overwrite").saveAsTable("genai_demo.cardinal_health.target_sales_report")

    logger.info("ETL process completed successfully.")

except Exception as e:
    logger.error(f"An error occurred during the ETL process: {e}")
    raise