import logging
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Load data from Unity Catalog tables
    hospital_stats_df = spark.table("genai_demo.cardinal_health.hospital_stats_north_america")
    hospital_assignments_df = spark.table("genai_demo.cardinal_health.hospital_sales_assignments")
    employment_details_df = spark.table("genai_demo.cardinal_health.sales_associates_employment_details")
    compensation_guidelines_df = spark.table("genai_demo.cardinal_health.compensation_guidelines")
    logistics_channels_df = spark.table("genai_demo.cardinal_health.logistics_channels")
    growth_opportunities_df = spark.table("genai_demo.cardinal_health.growth_opportunities")
    company_goals_df = spark.table("genai_demo.cardinal_health.company_goals")
    historical_sales_df = spark.table("genai_demo.cardinal_health.historical_sales")
    third_party_trends_df = spark.table("genai_demo.cardinal_health.third_party_sales_trends")

    # Step 1: Join hospital statistics with sales assignments
    hospital_sales_df = hospital_stats_df.join(
        hospital_assignments_df,
        on=["Hospital_ID", "Hospital_Name"],
        how="inner"
    )

    # Step 2: Join employment details with compensation guidelines
    employment_compensation_df = employment_details_df.join(
        compensation_guidelines_df,
        on="Associate_ID",
        how="inner"
    )

    # Step 3: Calculate total compensation
    employment_compensation_df = employment_compensation_df.withColumn(
        "Compensation",
        F.col("Base_Salary") + (F.col("Commission_Percentage") / 100 * F.col("Base_Salary")) + F.col("Bonus")
    )

    # Step 4: Select relevant fields
    selected_employment_df = employment_compensation_df.select(
        "Associate_ID", "Associate_Name", "Compensation", "Department", "Employment_Type", "Region", "Years_of_Experience"
    )

    # Step 5: Join logistics channels with growth opportunities
    logistics_growth_df = logistics_channels_df.join(
        growth_opportunities_df,
        on=["Channel_ID", "Channel_Type", "Hospital_ID"],
        how="inner"
    )

    # Step 6: Join compensation data with logistics and growth data
    combined_df = selected_employment_df.join(
        logistics_growth_df,
        on="Hospital_ID",
        how="inner"
    )

    # Step 7: Select fields for further processing
    selected_combined_df = combined_df.select(
        "Hospital_ID", "Channel_Type", "Growth_Opportunities", "Projected_Growth_Rate"
    )

    # Step 8: Join selected data with company goals
    goals_combined_df = selected_combined_df.join(
        company_goals_df,
        on=["Hospital_ID", "Channel_Type"],
        how="inner"
    )

    # Step 9: Ensure unique records
    unique_goals_df = goals_combined_df.dropDuplicates(["Hospital_ID", "Channel_Type", "Projected_Growth_Rate", "Investment_Planned"])

    # Step 10: Join historical sales with third-party sales trends
    sales_trends_df = historical_sales_df.join(
        third_party_trends_df,
        on="Channel_Type",
        how="inner"
    )

    # Step 11: Ensure unique sales records
    unique_sales_df = sales_trends_df.dropDuplicates(["Year", "Channel_Type", "Sales_Revenue"])

    # Step 12: Join unique sales data with growth and investment data
    final_df = unique_sales_df.join(
        unique_goals_df,
        on=["Hospital_ID", "Channel_ID", "Channel_Type"],
        how="inner"
    )

    # Step 13: Generate rows for target years
    final_df = final_df.withColumn("Target Year", F.expr("sequence(2023, 2026)"))

    # Step 14: Calculate projected sales growth rate
    final_df = final_df.withColumn(
        "projected_sales_growth_rate",
        F.when(F.col("Target Year") == 2024, F.col("Projected_Growth_Rate") + (F.col("Projected_Growth_Rate") / 100))
        .when(F.col("Target Year") == 2025, (F.col("Projected_Growth_Rate") + (F.col("Projected_Growth_Rate") / 100)) + (F.col("Projected_Growth_Rate") / 100))
        .when(F.col("Target Year") == 2026, ((F.col("Projected_Growth_Rate") + (F.col("Projected_Growth_Rate") / 100)) + (F.col("Projected_Growth_Rate") / 100)) + (F.col("Projected_Growth_Rate") / 100))
        .otherwise(F.col("Projected_Growth_Rate"))
    )

    # Step 15: Calculate projected investments
    final_df = final_df.withColumn(
        "projected_investments",
        F.when(F.col("Target Year") == 2024, F.col("Investment_Planned") * (F.col("projected_sales_growth_rate") / 100))
        .when(F.col("Target Year") == 2025, F.col("Investment_Planned") * (1 + F.col("projected_sales_growth_rate") / 100))
        .when(F.col("Target Year") == 2026, F.col("Investment_Planned") * (1 + F.col("projected_sales_growth_rate") / 100))
        .otherwise(F.col("Investment_Planned"))
    )

    # Step 16: Calculate projected revenue
    final_df = final_df.withColumn(
        "Projected Revenue",
        F.when(F.col("Target Year") == 2024, F.col("Sales_Revenue") * (F.col("projected_sales_growth_rate") / 100))
        .when(F.col("Target Year") == 2025, F.col("Sales_Revenue") * (1 + F.col("projected_sales_growth_rate") / 100))
        .when(F.col("Target Year") == 2026, F.col("Sales_Revenue") * (1 + F.col("projected_sales_growth_rate") / 100))
        .otherwise(F.col("Sales_Revenue"))
    )

    # Step 17: Filter data for target years greater than 2023
    filtered_df = final_df.filter(F.col("Target Year") > 2023)

    # Step 18: Select final fields for output
    output_df = filtered_df.select(
        "Channel_Type", "Hospital_ID", "Market_Trend", "Political_Impact", "Economic_Impact", "Target Year",
        "projected_sales_growth_rate", "projected_investments", "Projected Revenue"
    )

    # Step 19: Sort data by target year in ascending order
    sorted_output_df = output_df.orderBy("Target Year")

    # Step 20: Write the processed data to Unity Catalog tables using Delta format
    sorted_output_df.write.format("delta").mode("overwrite").saveAsTable("genai_demo.cardinal_health.sales_prediction_output")

    logger.info("ETL process completed successfully.")

except Exception as e:
    logger.error(f"An error occurred during the ETL process: {e}")