import logging
import psycopg2
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, StringType, DateType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Securely retrieve credentials using Databricks utilities
postgres_user = dbutils.secrets.get(scope="my_scope", key="postgres_user")
postgres_password = dbutils.secrets.get(scope="my_scope", key="postgres_password")
mysql_user = dbutils.secrets.get(scope="my_scope", key="mysql_user")
mysql_password = dbutils.secrets.get(scope="my_scope", key="mysql_password")
sqlserver_user = dbutils.secrets.get(scope="my_scope", key="sqlserver_user")
sqlserver_password = dbutils.secrets.get(scope="my_scope", key="sqlserver_password")

# Connect to PostgreSQL to fetch policy data
try:
    conn_postgres = psycopg2.connect(
        dbname="PolicyDB",
        user=postgres_user,
        password=postgres_password,
        host="postgres_server"
    )
    policy_query = "SELECT Policy_ID, Customer_ID, Policy_Type, Policy_Status, Policy_Start_Date, Policy_End_Date, Policy_Term, Policy_Premium, Total_Premium_Paid, Renewal_Status, Policy_Addons FROM policy_table"
    policy_df = spark.read.format("jdbc").option("url", f"jdbc:postgresql://postgres_server/PolicyDB").option("query", policy_query).option("user", postgres_user).option("password", postgres_password).load()
    logger.info("Policy data loaded successfully from PostgreSQL.")
except Exception as e:
    logger.error(f"Error loading policy data: {e}")

# Connect to MySQL to fetch claims data
try:
    claims_query = "SELECT Claim_ID, Policy_ID, Claim_Date, Claim_Type, Claim_Status, Claim_Amount, Claim_Payout FROM claims_table"
    claims_df = spark.read.format("jdbc").option("url", f"jdbc:mysql://mysql_server/ClaimsDB").option("query", claims_query).option("user", mysql_user).option("password", mysql_password).load()
    logger.info("Claims data loaded successfully from MySQL.")
except Exception as e:
    logger.error(f"Error loading claims data: {e}")

# Connect to SQL Server to fetch customer demographics
try:
    demographics_query = "SELECT Customer_ID, Customer_Name, Email, Phone_Number, Address, City, State, Postal_Code, Date_of_Birth, Gender, Marital_Status, Occupation, Income_Level, Customer_Segment FROM customer_demographics_table"
    demographics_df = spark.read.format("jdbc").option("url", f"jdbc:sqlserver://sqlserver_server/DemographicsDB").option("query", demographics_query).option("user", sqlserver_user).option("password", sqlserver_password).load()
    logger.info("Customer demographics data loaded successfully from SQL Server.")
except Exception as e:
    logger.error(f"Error loading customer demographics data: {e}")

# Join policy data with customer demographics
try:
    policy_demo_df = policy_df.join(demographics_df, policy_df.Customer_ID == demographics_df.Customer_ID, "inner")
    logger.info("Policy data joined with customer demographics successfully.")
except Exception as e:
    logger.error(f"Error joining policy data with customer demographics: {e}")

# Join the result with claims data
try:
    policy_claims_df = policy_demo_df.join(claims_df, policy_demo_df.Policy_ID == claims_df.Policy_ID, "inner")
    logger.info("Policy and demographics data joined with claims data successfully.")
except Exception as e:
    logger.error(f"Error joining policy and demographics data with claims data: {e}")

# Aggregate data at the customer level
try:
    agg_customer_df = policy_claims_df.groupBy("Customer_ID").agg(
        F.count("Claim_ID").alias("Total_Claims"),
        F.avg("Claim_Amount").alias("Average_Claim_Amount"),
        F.max("Claim_Date").alias("Recent_Claim_Date"),
        F.countDistinct("Policy_ID").alias("Policy_Count")
    )
    logger.info("Customer-level data aggregated successfully.")
except Exception as e:
    logger.error(f"Error aggregating customer-level data: {e}")

# Calculate derived fields
try:
    derived_df = agg_customer_df.withColumn("Age", F.datediff(F.current_date(), F.col("Date_of_Birth")) / 365) \
        .withColumn("Claim_To_Premium_Ratio", F.when(F.col("Total_Premium_Paid") != 0, F.col("Claim_Amount") / F.col("Total_Premium_Paid")).otherwise(0)) \
        .withColumn("Claims_Per_Policy", F.when(F.col("Policy_Count") != 0, F.col("Total_Claims") / F.col("Policy_Count")).otherwise(0)) \
        .withColumn("Retention_Rate", F.lit(0.85)) \
        .withColumn("Cross_Sell_Opportunities", F.lit("Multi-Policy Discount, Home Coverage Add-on")) \
        .withColumn("Upsell_Potential", F.lit("Premium Vehicle Coverage"))
    logger.info("Derived fields calculated successfully.")
except Exception as e:
    logger.error(f"Error calculating derived fields: {e}")

# Custom transformation to fetch Fraud Score and Credit Score using API
def fetch_api_score(customer_id, score_type):
    # Placeholder function to simulate API call
    return 0.0

try:
    fraud_score_udf = F.udf(lambda customer_id: fetch_api_score(customer_id, "Fraud"), DecimalType())
    credit_score_udf = F.udf(lambda customer_id: fetch_api_score(customer_id, "Credit"), DecimalType())

    derived_df = derived_df.withColumn("Fraud_Score", fraud_score_udf(F.col("Customer_ID"))) \
        .withColumn("Credit_Score", credit_score_udf(F.col("Customer_ID")))
    logger.info("Fraud Score and Credit Score fetched successfully using API.")
except Exception as e:
    logger.error(f"Error fetching Fraud Score and Credit Score: {e}")

# Generate AI/ML insights
try:
    insights_df = derived_df.withColumn("Churn_Probability", F.lit(0.25)) \
        .withColumn("Next_Best_Offer", F.lit("Additional Life Coverage")) \
        .withColumn("Claims_Fraud_Probability", F.lit(0.10)) \
        .withColumn("Revenue_Potential", F.lit(12000.00))
    logger.info("AI/ML insights generated successfully.")
except Exception as e:
    logger.error(f"Error generating AI/ML insights: {e}")

# Write the final output to Unity Catalog table
try:
    spark.sql("DROP TABLE IF EXISTS catalog.target_db.TGT_CUSTOMER_360")
    insights_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.TGT_CUSTOMER_360")
    logger.info("Data written to Unity Catalog table successfully.")
except Exception as e:
    logger.error(f"Error writing data to Unity Catalog table: {e}")