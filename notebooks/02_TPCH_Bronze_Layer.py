# Databricks notebook source
current_catalog = spark.sql("SELECT current_catalog()").first()[0]
print("Current catalog:", current_catalog)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{current_catalog}`.tpch_bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{current_catalog}`.tpch_silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{current_catalog}`.tpch_gold")

print("Bronze, Silver and Gold schemas created successfully.")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

orders_bronze = (
    spark.read.table("samples.tpch.orders")
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_table", lit("samples.tpch.orders"))
)

orders_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"`{current_catalog}`.tpch_bronze.orders")

print("Bronze orders table created.")

# COMMAND ----------

lineitem_bronze = (
    spark.read.table("samples.tpch.lineitem")
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_table", lit("samples.tpch.lineitem"))
)

lineitem_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"`{current_catalog}`.tpch_bronze.lineitem")

print("Bronze lineitem table created.")

# COMMAND ----------

customer_bronze = (
    spark.read.table("samples.tpch.customer")
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_table", lit("samples.tpch.customer"))
)

customer_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"`{current_catalog}`.tpch_bronze.customer")

print("Bronze customer table created.")

# COMMAND ----------

nation_bronze = (
    spark.read.table("samples.tpch.nation")
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_table", lit("samples.tpch.nation"))
)

nation_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"`{current_catalog}`.tpch_bronze.nation")

# COMMAND ----------

region_bronze = (
    spark.read.table("samples.tpch.region")
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_table", lit("samples.tpch.region"))
)

region_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"`{current_catalog}`.tpch_bronze.region")

# COMMAND ----------

display(
    spark.sql(
        f"SHOW TABLES IN `{current_catalog}`.tpch_bronze"
    )
)

# COMMAND ----------

display(
    spark.read.table(
        f"`{current_catalog}`.tpch_bronze.orders"
    ).limit(10)
)