import logging
from pyspark.sql import functions as F

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Load data from Unity Catalog tables
    orders_central_df = spark.table("catalog.source_db.orders_central")
    orders_west_df = spark.table("catalog.source_db.orders_west")
    orders_east_df = spark.table("catalog.source_db.orders_east")
    orders_south_df = spark.table("catalog.source_db.orders_south_2015")
    quota_df = spark.table("catalog.source_db.quota")
    returns_df = spark.table("catalog.source_db.returns")

    # Fix Dates for Orders (Central)
    orders_central_df = orders_central_df.withColumn("Region", F.lit("Central")) \
        .withColumn("Order Date", F.to_date(F.concat_ws("/", F.col("Order Day"), F.col("Order Month"), F.col("Order Year")), "dd/MM/yyyy")) \
        .withColumn("Ship Date", F.to_date(F.concat_ws("/", F.col("Ship Day"), F.col("Ship Month"), F.col("Ship Year")), "dd/MM/yyyy")) \
        .drop("Order Year", "Order Month", "Order Day", "Ship Year", "Ship Month", "Ship Day") \
        .withColumnRenamed("Discounts", "Discount") \
        .withColumnRenamed("Product", "Product Name")

    # Remove Nulls
    orders_central_df = orders_central_df.filter(F.col("Order ID").isNotNull())

    # Fix Data Type
    orders_central_df = orders_central_df.withColumn("Discount", F.col("Discount").cast("string")) \
        .withColumn("Sales", F.regexp_replace(F.col("Sales"), "[^0-9.]", "").cast("double"))

    # Rename States
    state_mapping = {
        "Arizona": "AZ", "California": "CA", "Colorado": "CO", "Idaho": "ID",
        "Montana": "MT", "New Mexico": "NM", "Oregon": "OR", "Washington": "WA", "Utah": "UT"
    }
    orders_central_df = orders_central_df.replace(state_mapping, subset=["State"])

    # Pivot Quotas
    quota_df = quota_df.selectExpr("Region", "stack(4, '2014', 2014, '2015', 2015, '2016', 2016, '2017', 2017) as (Year, Quota)") \
        .withColumn("Year", F.col("Year").cast("integer"))

    # All Orders
    all_orders_df = orders_central_df.union(orders_west_df).union(orders_east_df).union(orders_south_df)

    # Orders + Returns
    orders_returns_df = all_orders_df.join(returns_df, ["Product ID", "Order ID"], "right") \
        .withColumn("Discount", F.col("Discount").cast("double")) \
        .withColumn("Days to Ship", F.datediff(F.col("Ship Date"), F.col("Order Date"))) \
        .withColumn("Returned?", F.when(F.col("Return Reason").isNotNull(), "Yes").otherwise("No")) \
        .withColumn("Discount", F.when(F.col("Discount").isNull(), 0).otherwise(F.col("Discount"))) \
        .withColumn("Year of Sale", F.year(F.col("Order Date"))) \
        .filter(~((F.col("Discount") >= 17) & (F.col("Discount") < 18)))

    # Clean Notes/Approver
    orders_returns_df = orders_returns_df.withColumn("Return Notes", F.trim(F.split(F.col("Notes"), "-", 1)[0])) \
        .withColumn("Approver", F.trim(F.split(F.col("Notes"), "-", 2)[1])) \
        .drop("Notes")

    approver_mapping = {
        "M Gomez": "M Gomez", "S Kelly": "S Kelly", "F Azad": "F Azad",
        "L Smith": "L Smith", "G Lindsay": "G Lindsay", "R Chen": "R Chen",
        "K Lawrence": "K Lawrence", "L Jenkins": "L Jenkins", "R Duchesne": "R Duchesne"
    }
    orders_returns_df = orders_returns_df.replace(approver_mapping, subset=["Approver"])

    # Clean 2
    orders_returns_df = orders_returns_df.drop("Order ID-1", "Product ID").withColumnRenamed("Product ID-1", "Product ID")

    # Roll Up Sales
    rolled_up_sales_df = orders_returns_df.groupBy("Region", "Year of Sale") \
        .agg(F.sum("Profit").alias("Profit"), F.sum("Sales").alias("Sales"), F.sum("Quantity").alias("Quantity"), F.avg("Discount").alias("Discount"))

    # Quota + Orders
    quota_orders_df = rolled_up_sales_df.join(quota_df, ["Region", "Year"], "inner")

    # Write outputs to Unity Catalog tables
    spark.sql("DROP TABLE IF EXISTS catalog.target_db.annual_regional_performance")
    quota_orders_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.annual_regional_performance")

    spark.sql("DROP TABLE IF EXISTS catalog.target_db.superstore_sales")
    orders_returns_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.superstore_sales")

    logger.info("ETL process completed successfully.")

except Exception as e:
    logger.error(f"Error during ETL process: {e}")