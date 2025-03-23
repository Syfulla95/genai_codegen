"import logging\nfrom pyspark.sql import functions as F\nfrom pyspark.sql.types import IntegerType, DateType, StringType\n\n# Configure logging\nlogging.basicConfig(level=logging.INFO)\nlogger = logging.getLogger(__name__)\n\ntry:\n    # Load data from Unity Catalog tables\n    logger.info(\"Loading data from Unity Catalog tables...\")\n    orders_central_df = spark.table(\"catalog.sales.orders_central\")\n    orders_west_df = spark.table(\"catalog.sales.orders_west\")\n    orders_east_df = spark.table(\"catalog.sales.orders_east\")\n    orders_south_df = spark.table(\"catalog.sales.orders_south\")\n    quota_df = spark.table(\"catalog.sales.quota\")\n    returns_df = spark.table(\"catalog.sales.returns\")\n\n    # Step 1: Fix Dates and Add Region\n    logger.info(\"Fixing dates and adding region information...\")\n    orders_central_df = (\n        orders_central_df\n        .withColumn(\"Region\", F.lit(\"Central\"))\n        .withColumn(\"Order Date\", F.concat_ws(\"-\", F.col(\"Order Year\"), F.col(\"Order Month\"), F.col(\"Order Day\")).cast(DateType()))\n        .withColumn(\"Ship Date\", F.concat_ws(\"-\", F.col(\"Ship Year\"), F.col(\"Ship Month\"), F.col(\"Ship Day\")).cast(DateType()))\n        .drop(\"Order Year\", \"Order Month\", \"Order Day\", \"Ship Year\", \"Ship Month\", \"Ship Day\")\n        .withColumnRenamed(\"Discounts\", \"Discount\")\n        .withColumnRenamed(\"Product\", \"Product Name\")\n    )\n\n    # Step 2: Remove Nulls\n    logger.info(\"Removing null Order IDs...\")\n    orders_central_df = orders_central_df.filter(F.col(\"Order ID\").isNotNull())\n\n    # Step 3: Fix Data Types\n    logger.info(\"Fixing data types...\")\n    orders_central_df = (\n        orders_central_df\n        .withColumn(\"Discount\", F.col(\"Discount\").cast(StringType()))\n        .withColumn(\"Sales\", F.regexp_replace(F.col(\"Sales\"), \"[^0-9.]\", \"\").cast(\"double\"))\n    )\n\n    # Step 4: Rename States\n    logger.info(\"Renaming states to abbreviations...\")\n    state_abbreviations = {\n        \"California\": \"CA\",\n        \"New York\": \"NY\",\n        # Add more state mappings as needed\n    }\n    orders_central_df = orders_central_df.replace(state_abbreviations, subset=[\"State\"])\n\n    # Step 5: Pivot Quotas\n    logger.info(\"Pivoting quota data...\")\n    quota_df = (\n        quota_df\n        .selectExpr(\"Region\", \"stack(4, '2015', `2015`, '2016', `2016`, '2017', `2017`, '2018', `2018`) as (Year, Quota)\")\n        .withColumn(\"Year\", F.col(\"Year\").cast(IntegerType()))\n    )\n\n    # Step 6: Union All Orders\n    logger.info(\"Unioning all orders data...\")\n    all_orders_df = (\n        orders_central_df\n        .unionByName(orders_west_df)\n        .unionByName(orders_east_df)\n        .unionByName(orders_south_df)\n    )\n\n    # Cache the unioned DataFrame for performance optimization\n    all_orders_df.cache()\n\n    # Step 7: Join Orders with Returns\n    logger.info(\"Joining orders with returns data...\")\n    orders_returns_df = (\n        all_orders_df.alias(\"orders\")\n        .join(returns_df.alias(\"returns\"), [\"Order ID\", \"Product ID\"], \"left\")\n        .withColumn(\"Returned?\", F.when(F.col(\"Return Reason\").isNotNull(), \"Yes\").otherwise(\"No\"))\n        .withColumn(\"Days to Ship\", F.datediff(F.col(\"Ship Date\"), F.col(\"Order Date\")))\n        .withColumn(\"Discount\", F.when(F.col(\"Discount\").isNull(), 0).otherwise(F.col(\"Discount\")))\n        .withColumn(\"Year of Sale\", F.year(F.col(\"Order Date\")))\n        .filter(~((F.col(\"Discount\") >= 17) & (F.col(\"Discount\") <= 18)))\n    )\n\n    # Step 8: Clean Notes/Approver\n    logger.info(\"Cleaning notes and approver fields...\")\n    orders_returns_df = (\n        orders_returns_df\n        .withColumn(\"Return Notes\", F.split(F.col(\"Notes\"), \"-\")[0])\n        .withColumn(\"Approver\", F.split(F.col(\"Notes\"), \"-\")[1])\n        .drop(\"Notes\")\n        .withColumn(\"Approver\", F.trim(F.col(\"Approver\")))\n    )\n\n    # Step 9: Roll Up Sales\n    logger.info(\"Aggregating sales data...\")\n    aggregated_sales_df = (\n        orders_returns_df\n        .groupBy(\"Region\", \"Year of Sale\")\n        .agg(\n            F.sum(\"Profit\").alias(\"Total Profit\"),\n            F.sum(\"Sales\").alias(\"Total Sales\"),\n            F.sum(\"Quantity\").alias(\"Total Quantity\"),\n            F.avg(\"Discount\").alias(\"Average Discount\")\n        )\n    )\n\n    # Step 10: Join Quota with Orders\n    logger.info(\"Joining quota with aggregated sales data...\")\n    final_df = aggregated_sales_df.join(quota_df, [\"Region\", \"Year of Sale\"], \"inner\")\n\n    # Output: Write to Unity Catalog tables\n    logger.info(\"Writing transformed data to Unity Catalog tables...\")\n    spark.sql(\"DROP TABLE IF EXISTS catalog.sales.annual_regional_performance\")\n    final_df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"catalog.sales.annual_regional_performance\")\n\n    spark.sql(\"DROP TABLE IF EXISTS catalog.sales.superstore_sales\")\n    orders_returns_df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"catalog.sales.superstore_sales\")\n\n    logger.info(\"ETL process completed successfully.\")\n\nexcept Exception as e:\n    logger.error(f\"An error occurred during the ETL process: {e}\", exc_info=True)\n    raise\n"