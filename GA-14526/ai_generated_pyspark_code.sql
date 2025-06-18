import logging
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Load data from Unity Catalog tables
    policy_df = spark.table("genai_demo.jnj.policy")
    claims_df = spark.table("genai_demo.jnj.claims")
    demographics_df = spark.table("genai_demo.jnj.demographics")
    scores_df = spark.table("genai_demo.jnj.scores")
    aiml_insights_df = spark.table("genai_demo.jnj.aiml_insights")

    # Select relevant fields from each dataset
    demographics_selected_df = demographics_df.select(
        "Customer_ID", "Customer_Name", "Email", "Phone_Number", "Address", "City", "State", "Postal_Code",
        "Date_of_Birth", "Gender", "Marital_Status", "Occupation", "Income_Level", "Customer_Segment"
    )

    claims_selected_df = claims_df.select(
        "Claim_ID", "Policy_ID", "Claim_Date", "Claim_Type", "Claim_Status", "Claim_Amount", "Claim_Payout"
    )

    policy_selected_df = policy_df.select(
        "Policy_ID", "Customer_ID", "Policy_Type", "Policy_Status", "Policy_Start_Date", "Policy_End_Date",
        "Policy_Term", "Policy_Premium", "Total_Premium_Paid", "Renewal_Status", "Policy_Addons"
    )

    aiml_insights_selected_df = aiml_insights_df.select(
        "Customer_ID", "Churn_Probability", "Next_Best_Offer", "Claims_Fraud_Probability", "Revenue_Potential"
    )

    scores_selected_df = scores_df.select(
        "Customer_ID", "Credit_Score", "Fraud_Score", "Customer_Risk_Score"
    )

    # Join demographics and policy data on Customer_ID
    demographics_policy_joined_df = demographics_selected_df.join(
        policy_selected_df, "Customer_ID", "inner"
    )

    # Join the result with claims data on Policy_ID
    all_joined_df = demographics_policy_joined_df.join(
        claims_selected_df, "Policy_ID", "inner"
    )

    # Summarize data to calculate total claims, recent claim date, average claim amount, and policy count per customer
    summarized_df = all_joined_df.groupBy("Customer_ID").agg(
        F.count("Claim_ID").alias("Total_Claims"),
        F.max("Claim_Date").alias("Recent_Claim_Date"),
        F.avg("Claim_Amount").alias("Average_Claim_Amount"),
        F.countDistinct("Policy_ID").alias("Policy_Count")
    )

    # Join summarized data with detailed customer data
    detailed_customer_df = all_joined_df.join(
        summarized_df, "Customer_ID", "inner"
    )

    # Calculate additional metrics
    detailed_customer_df = detailed_customer_df.withColumn(
        "Age", F.datediff(F.current_date(), F.to_date("Date_of_Birth", "yyyy-MM-dd")) / 365
    ).withColumn(
        "Claim_To_Premium_Ratio", F.when(
            detailed_customer_df["Total_Premium_Paid"] != 0,
            detailed_customer_df["Claim_Amount"] / detailed_customer_df["Total_Premium_Paid"]
        ).otherwise(0)
    ).withColumn(
        "Claims_Per_Policy", F.when(
            detailed_customer_df["Policy_Count"] != 0,
            detailed_customer_df["Total_Claims"] / detailed_customer_df["Policy_Count"]
        ).otherwise(0)
    ).withColumn(
        "Retention_Rate", F.lit(0.85)
    ).withColumn(
        "Cross_Sell_Opportunities", F.lit("Multi-Policy Discount, Home Coverage Add-on")
    ).withColumn(
        "Upsell_Potential", F.lit("Premium Vehicle Coverage")
    )

    # Join all data sources into a single dataset
    final_df = detailed_customer_df.join(
        aiml_insights_selected_df, "Customer_ID", "inner"
    ).join(
        scores_selected_df, "Customer_ID", "inner"
    )

    # Drop existing table if necessary
    spark.sql("DROP TABLE IF EXISTS genai_demo.jnj.final_output")

    # Write the final DataFrame to Unity Catalog table
    final_df.write.format("delta").mode("overwrite").saveAsTable("genai_demo.jnj.final_output")

    logger.info("ETL process completed successfully and data written to Unity Catalog.")

except Exception as e:
    logger.error(f"An error occurred during the ETL process: {e}")