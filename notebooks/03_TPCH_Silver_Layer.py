# Databricks notebook source
from pyspark.sql import functions as F

current_catalog = spark.sql("SELECT current_catalog()").first()[0]
print("Current catalog:", current_catalog)

# COMMAND ----------

customer_df = spark.read.table(
    f"`{current_catalog}`.tpch_bronze.customer"
)

orders_df = spark.read.table(
    f"`{current_catalog}`.tpch_bronze.orders"
)

lineitem_df = spark.read.table(
    f"`{current_catalog}`.tpch_bronze.lineitem"
)

nation_df = spark.read.table(
    f"`{current_catalog}`.tpch_bronze.nation"
)

region_df = spark.read.table(
    f"`{current_catalog}`.tpch_bronze.region"
)

print("Bronze tables loaded successfully.")

# COMMAND ----------

print("Customer rows:", customer_df.count())
print("Orders rows:", orders_df.count())
print("Lineitem rows:", lineitem_df.count())
print("Nation rows:", nation_df.count())
print("Region rows:", region_df.count())

# COMMAND ----------

def null_profile(df):
    return df.select([
        F.sum(
            F.when(F.col(c).isNull(), 1).otherwise(0)
        ).alias(c)
        for c in df.columns
    ])

display(null_profile(customer_df))

# COMMAND ----------

display(null_profile(orders_df))

# COMMAND ----------

display(null_profile(lineitem_df))

# COMMAND ----------

customer_duplicates = (
    customer_df
    .groupBy("c_custkey")
    .count()
    .filter(F.col("count") > 1)
)

print("Duplicate customer keys:", customer_duplicates.count())

# COMMAND ----------

order_duplicates = (
    orders_df
    .groupBy("o_orderkey")
    .count()
    .filter(F.col("count") > 1)
)

print("Duplicate order keys:", order_duplicates.count())

# COMMAND ----------

lineitem_duplicates = (
    lineitem_df
    .groupBy("l_orderkey", "l_linenumber")
    .count()
    .filter(F.col("count") > 1)
)

print("Duplicate line items:", lineitem_duplicates.count())

# COMMAND ----------

silver_customer = (
    customer_df.alias("c")
    .join(
        nation_df.alias("n"),
        F.col("c.c_nationkey") == F.col("n.n_nationkey"),
        "left"
    )
    .join(
        region_df.alias("r"),
        F.col("n.n_regionkey") == F.col("r.r_regionkey"),
        "left"
    )
    .select(
        F.col("c.c_custkey").alias("customer_id"),
        F.col("c.c_name").alias("customer_name"),
        F.col("c.c_address").alias("address"),
        F.col("c.c_phone").alias("phone"),
        F.col("c.c_acctbal").alias("account_balance"),
        F.col("c.c_mktsegment").alias("market_segment"),
        F.col("n.n_name").alias("nation"),
        F.col("r.r_name").alias("region")
    )
)

# COMMAND ----------

display(silver_customer.limit(10))

# COMMAND ----------

silver_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        f"`{current_catalog}`.tpch_silver.customer"
    )

# COMMAND ----------

silver_orders = (
    orders_df
    .select(
        F.col("o_orderkey").alias("order_id"),
        F.col("o_custkey").alias("customer_id"),
        F.col("o_orderstatus").alias("order_status"),
        F.col("o_totalprice").alias("order_total"),
        F.col("o_orderdate").alias("order_date"),
        F.col("o_orderpriority").alias("order_priority"),
        F.col("o_shippriority").alias("ship_priority")
    )
    .withColumn(
        "order_year",
        F.year("order_date")
    )
    .withColumn(
        "order_month",
        F.month("order_date")
    )
)

# COMMAND ----------

display(silver_orders.limit(10))

# COMMAND ----------

silver_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        f"`{current_catalog}`.tpch_silver.orders"
    )

# COMMAND ----------

silver_lineitem = (
    lineitem_df
    .select(
        F.col("l_orderkey").alias("order_id"),
        F.col("l_linenumber").alias("line_number"),
        F.col("l_partkey").alias("part_id"),
        F.col("l_suppkey").alias("supplier_id"),
        F.col("l_quantity").alias("quantity"),
        F.col("l_extendedprice").alias("extended_price"),
        F.col("l_discount").alias("discount_rate"),
        F.col("l_tax").alias("tax_rate"),
        F.col("l_returnflag").alias("return_flag"),
        F.col("l_linestatus").alias("line_status"),
        F.col("l_shipdate").alias("ship_date"),
        F.col("l_commitdate").alias("commit_date"),
        F.col("l_receiptdate").alias("receipt_date"),
        F.col("l_shipmode").alias("ship_mode")
    )
    .withColumn(
        "discount_amount",
        F.round(
            F.col("extended_price") *
            F.col("discount_rate"),
            2
        )
    )
    .withColumn(
        "net_revenue",
        F.round(
            F.col("extended_price") *
            (1 - F.col("discount_rate")),
            2
        )
    )
    .withColumn(
        "revenue_with_tax",
        F.round(
            F.col("extended_price") *
            (1 - F.col("discount_rate")) *
            (1 + F.col("tax_rate")),
            2
        )
    )
)

# COMMAND ----------

display(
    silver_lineitem.select(
        "order_id",
        "quantity",
        "extended_price",
        "discount_rate",
        "discount_amount",
        "net_revenue",
        "revenue_with_tax"
    ).limit(10)
)

# COMMAND ----------

silver_lineitem = (
    silver_lineitem
    .withColumn(
        "shipping_days",
        F.datediff(
            F.col("receipt_date"),
            F.col("ship_date")
        )
    )
    .withColumn(
        "late_delivery",
        F.when(
            F.col("receipt_date") > F.col("commit_date"),
            1
        ).otherwise(0)
    )
)

# COMMAND ----------

display(
    silver_lineitem.select(
        "order_id",
        "ship_date",
        "commit_date",
        "receipt_date",
        "shipping_days",
        "late_delivery"
    ).limit(10)
)

# COMMAND ----------

silver_lineitem.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        f"`{current_catalog}`.tpch_silver.lineitem"
    )

# COMMAND ----------

display(
    spark.sql(
        f"SHOW TABLES IN `{current_catalog}`.tpch_silver"
    )
)