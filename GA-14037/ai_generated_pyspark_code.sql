import logging
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DateType, FloatType

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

    # Fix Dates for Central Orders
    orders_central_df = orders_central_df.withColumn("Region", F.lit("Central")) \
        .withColumn("Order Date", F.concat_ws("/", F.col("Order Day").cast(StringType()), F.col("Order Month").cast(StringType()), F.col("Order Year").cast(StringType()))) \
        .withColumn("Order Date", F.to_date(F.col("Order Date"), "dd/MM/yyyy")) \
        .withColumn("Ship Date", F.concat_ws("/", F.col("Ship Day").cast(StringType()), F.col("Ship Month").cast(StringType()), F.col("Ship Year").cast(StringType()))) \
        .withColumn("Ship Date", F.to_date(F.col("Ship Date"), "dd/MM/yyyy")) \
        .drop("Order Year", "Order Month", "Order Day", "Ship Year", "Ship Month", "Ship Day") \
        .withColumnRenamed("Discounts", "Discount") \
        .withColumnRenamed("Product", "Product Name")

    # Remove Nulls
    orders_central_df = orders_central_df.filter(F.col("Order ID").isNotNull())

    # Fix Data Type
    orders_central_df = orders_central_df.withColumn("Discount", F.col("Discount").cast(StringType())) \
        .withColumn("Sales", F.regexp_replace(F.col("Sales"), "[^0-9.]", "").cast(FloatType()))

    # Rename States
    state_mapping = {
        "AZ": "Arizona", "Arizona": "Arizona",
        "CA": "California", "California": "California",
        "CO": "Colorado", "Colorado": "Colorado",
        "ID": "Idaho", "Idaho": "Idaho",
        "MT": "Montana", "Montana": "Montana",
        "NM": "New Mexico", "New Mexico": "New Mexico",
        "OR": "Oregon", "Oregon": "Oregon",
        "WA": "Washington", "Washington": "Washington",
        "UT": "Utah", "Utah": "Utah"
    }
    orders_central_df = orders_central_df.replace(state_mapping, subset=["State"])

    # Pivot Quotas
    quota_df = quota_df.selectExpr("Region", "stack(4, '2015', 2015, '2016', 2016, '2017', 2017, '2018', 2018) as (Year, Quota)") \
        .withColumn("Year", F.col("Year").cast(IntegerType()))

    # Clean Notes/Approver
    returns_df = returns_df.withColumn("Return Notes", F.trim(F.split(F.col("Notes"), "-").getItem(0))) \
        .withColumn("Approver", F.trim(F.split(F.col("Notes"), "-").getItem(1))) \
        .drop("Notes")

    approver_mapping = {
        "M/ Gomez": "M Gomez", "M. Gomez": "M Gomez", "M Gomez": "M Gomez",
        "S Kelly": "S Kelly", "S. Kelly": "S Kelly",
        "F Azad": "F Azad", "F. Azad": "F Azad",
        "L Smith": "L Smith", "L. Smith": "L Smith",
        "G Lindsay": "G Lindsay", "G. Lindsay": "G Lindsay",
        "R Chen": "R Chen", "R. Chen": "R Chen",
        "K Lawrence": "K Lawrence", "K. Lawrence": "K Lawrence",
        "L Jenkins": "L Jenkins", "L. Jenkins": "L Jenkins",
        "R Duchesne": "R Duchesne", "R. Duchesne": "R Duchesne"
    }
    returns_df = returns_df.replace(approver_mapping, subset=["Approver"])

    # Roll Up Sales
    orders_df = orders_central_df.union(orders_west_df).union(orders_east_df).union(orders_south_df)
    rolled_up_sales_df = orders_df.groupBy("Region", "Year of Sale").agg(
        F.sum("Profit").alias("Profit"),
        F.sum("Sales").alias("Sales"),
        F.sum("Quantity").alias("Quantity"),
        F.avg("Discount").alias("Discount")
    )

    # Orders + Returns
    orders_returns_df = orders_df.join(returns_df, ["Order ID", "Product ID"], "right") \
        .withColumn("Returned?", F.when(F.col("Return Reason").isNotNull(), "Yes").otherwise("No")) \
        .withColumn("Days to Ship", F.datediff(F.col("Ship Date"), F.col("Order Date"))) \
        .withColumn("Discount", F.coalesce(F.col("Discount"), F.lit(0))) \
        .withColumn("Year of Sale", F.year(F.col("Order Date"))) \
        .filter(~((F.col("Discount") >= 17) & (F.col("Discount") < 18))) \
        .drop("Table Names", "File Paths")

    # Quota + Orders
    quota_orders_df = quota_df.join(rolled_up_sales_df, ["Region", "Year"], "inner")

    # Write outputs to Unity Catalog tables
    spark.sql("DROP TABLE IF EXISTS catalog.target_db.annual_regional_performance")
    quota_orders_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.annual_regional_performance")

    spark.sql("DROP TABLE IF EXISTS catalog.target_db.superstore_sales")
    orders_returns_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.superstore_sales")

    logger.info("Data migration completed successfully.")

except Exception as e:
    logger.error(f"Error during data migration: {e}")