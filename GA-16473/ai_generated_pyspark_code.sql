import logging
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Load data from Unity Catalog tables
    policy_df = spark.table("catalog.insurance_db.policy")
    claims_df = spark.table("catalog.insurance_db.claims")
    demographics_df = spark.table("catalog.insurance_db.demographics")
    scores_df = spark.table("catalog.insurance_db.scores")
    aiml_insights_df = spark.table("catalog.insurance_db.aiml_insights")

    # Select relevant fields from each DataFrame
    demographics_selected_df = demographics_df.select(
        "Customer_ID", "Customer_Name", "Email", "Phone_Number", "Address", "City", "State", "Postal_Code",
        "Date_of_Birth", "Gender", "Marital_Status", "Occupation", "Income_Level", "Customer_Segment"
    )

    claims_selected_df = claims_df.select(
        "Claim_ID", "Policy_ID", "Claim_Date", "Claim_Type", "Claim_Status", "Claim_Amount", "Claim_Payout"
    )

    policy_selected_df = policy_df.select(
        "policy_id", "customer_id", "policy_type", "policy_status", "policy_start_date", "policy_end_date",
        "policy_term", "policy_premium", "total_premium_paid", "renewal_status", "policy_addons"
    )

    scores_selected_df = scores_df.select(
        "Customer_ID", "Credit_Score", "Fraud_Score", "Customer_Risk_Score"
    )

    aiml_insights_selected_df = aiml_insights_df.select(
        "Customer_ID", "Churn_Probability", "Next_Best_Offer", "Claims_Fraud_Probability", "Revenue_Potential"
    )

    # Join demographics and policy data on customer ID
    demographics_policy_joined_df = demographics_selected_df.join(
        policy_selected_df, demographics_selected_df.Customer_ID == policy_selected_df.customer_id, "inner"
    )

    # Join the result with claims data on policy ID
    demographics_policy_claims_joined_df = demographics_policy_joined_df.join(
        claims_selected_df, demographics_policy_joined_df.policy_id == claims_selected_df.Policy_ID, "inner"
    )

    # Summarize data to calculate total claims, policy count, recent claim date, and average claim amount
    summarized_df = demographics_policy_claims_joined_df.groupBy("Customer_ID").agg(
        F.count("Claim_ID").alias("Total_Claims"),
        F.count("policy_id").alias("Policy_Count"),
        F.max("Claim_Date").alias("Recent_Claim_Date"),
        F.avg("Claim_Amount").alias("Average_Claim_Amount")
    )

    # Join summarized data with the previous join result on customer ID
    final_joined_df = demographics_policy_claims_joined_df.join(
        summarized_df, "Customer_ID", "inner"
    )

    # Custom Calculations
    final_calculated_df = final_joined_df.withColumn(
        "Age", F.floor(F.datediff(F.current_date(), F.to_date("Date_of_Birth", "yyyy-MM-dd")) / 365)
    ).withColumn(
        "Claim_To_Premium_Ratio", F.when(final_joined_df.total_premium_paid != 0,
                                         final_joined_df.Claim_Amount / final_joined_df.total_premium_paid).otherwise(0)
    ).withColumn(
        "Claims_Per_Policy", F.when(final_joined_df.Policy_Count != 0,
                                    final_joined_df.Total_Claims / final_joined_df.Policy_Count).otherwise(0)
    ).withColumn(
        "Retention_Rate", F.lit(0.85)
    ).withColumn(
        "Cross_Sell_Opportunities", F.lit("Multi-Policy Discount, Home Coverage Add-on")
    ).withColumn(
        "Upsell_Potential", F.lit("Premium Vehicle Coverage")
    )

    # Join with AI/ML insights and scores data
    customer_360_df = final_calculated_df.join(
        aiml_insights_selected_df, "Customer_ID", "inner"
    ).join(
        scores_selected_df, "Customer_ID", "inner"
    )

    # Write the final DataFrame to Unity Catalog as a Delta table
    spark.sql("DROP TABLE IF EXISTS catalog.insurance_db.customer_360")
    customer_360_df.write.format("delta").mode("overwrite").saveAsTable("catalog.insurance_db.customer_360")

    logger.info("ETL process completed successfully and data written to Unity Catalog.")

except Exception as e:
    logger.error(f"An error occurred during the ETL process: {str(e)}")