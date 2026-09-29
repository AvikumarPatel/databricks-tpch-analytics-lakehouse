# Databricks notebook source
from pyspark.sql import functions as F

current_catalog = spark.sql("SELECT current_catalog()").first()[0]

customer = spark.read.table(
    f"`{current_catalog}`.tpch_silver.customer"
)

orders = spark.read.table(
    f"`{current_catalog}`.tpch_silver.orders"
)

lineitem = spark.read.table(
    f"`{current_catalog}`.tpch_silver.lineitem"
)

print("Silver tables loaded successfully.")

# COMMAND ----------

order_line_metrics = (
    lineitem
    .groupBy("order_id")
    .agg(
        F.sum("extended_price").alias("gross_revenue"),
        F.sum("discount_amount").alias("discount_amount"),
        F.sum("net_revenue").alias("net_revenue"),
        F.sum("revenue_with_tax").alias("revenue_with_tax"),
        F.sum("quantity").alias("total_quantity"),
        F.avg("shipping_days").alias("avg_shipping_days"),
        F.max("late_delivery").alias("late_delivery_flag"),
        F.count("*").alias("line_item_count")
    )
)

display(order_line_metrics.limit(10))

# COMMAND ----------

gold_order_performance = (
    orders.alias("o")
    .join(
        order_line_metrics.alias("l"),
        F.col("o.order_id") == F.col("l.order_id"),
        "inner"
    )
    .join(
        customer.alias("c"),
        F.col("o.customer_id") == F.col("c.customer_id"),
        "left"
    )
    .select(
        F.col("o.order_id"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("o.order_year"),
        F.col("o.order_month"),
        F.col("o.order_status"),
        F.col("o.order_priority"),

        F.col("c.market_segment"),
        F.col("c.nation"),
        F.col("c.region"),

        F.col("l.total_quantity"),
        F.col("l.line_item_count"),

        F.round("l.gross_revenue", 2).alias("gross_revenue"),
        F.round("l.discount_amount", 2).alias("discount_amount"),
        F.round("l.net_revenue", 2).alias("net_revenue"),
        F.round("l.revenue_with_tax", 2).alias("revenue_with_tax"),

        F.round("l.avg_shipping_days", 2).alias("avg_shipping_days"),
        F.col("l.late_delivery_flag")
    )
)

# COMMAND ----------

display(gold_order_performance.limit(10))

# COMMAND ----------

gold_order_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        f"`{current_catalog}`.tpch_gold.order_performance"
    )

# COMMAND ----------

executive_kpis = (
    gold_order_performance
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("total_customers"),

        F.round(
            F.sum("gross_revenue"), 2
        ).alias("gross_revenue"),

        F.round(
            F.sum("discount_amount"), 2
        ).alias("total_discount"),

        F.round(
            F.sum("net_revenue"), 2
        ).alias("net_revenue"),

        F.round(
            F.avg("net_revenue"), 2
        ).alias("avg_order_value"),

        F.round(
            F.avg("avg_shipping_days"), 2
        ).alias("avg_shipping_days"),

        F.round(
            F.avg("late_delivery_flag") * 100,
            2
        ).alias("late_delivery_rate_pct")
    )
)

# COMMAND ----------

display(executive_kpis)

# COMMAND ----------

executive_kpis.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        f"`{current_catalog}`.tpch_gold.executive_kpis"
    )

# COMMAND ----------

monthly_revenue = (
    gold_order_performance
    .groupBy(
        "order_year",
        "order_month"
    )
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.round(
            F.sum("net_revenue"), 2
        ).alias("net_revenue"),
        F.round(
            F.avg("net_revenue"), 2
        ).alias("avg_order_value")
    )
    .orderBy(
        "order_year",
        "order_month"
    )
)

# COMMAND ----------

display(monthly_revenue)

# COMMAND ----------

monthly_revenue.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        f"`{current_catalog}`.tpch_gold.monthly_revenue"
    )

# COMMAND ----------

regional_performance = (
    gold_order_performance
    .groupBy("region")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),

        F.countDistinct("customer_id").alias("customers"),

        F.round(
            F.sum("net_revenue"), 2
        ).alias("net_revenue"),

        F.round(
            F.avg("net_revenue"), 2
        ).alias("avg_order_value"),

        F.round(
            F.avg("late_delivery_flag") * 100,
            2
        ).alias("late_delivery_rate_pct")
    )
    .orderBy(
        F.desc("net_revenue")
    )
)

# COMMAND ----------

display(regional_performance)

# COMMAND ----------

regional_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        f"`{current_catalog}`.tpch_gold.regional_performance"
    )

# COMMAND ----------

segment_performance = (
    gold_order_performance
    .groupBy("market_segment")
    .agg(
        F.countDistinct("customer_id").alias("customers"),

        F.countDistinct("order_id").alias("total_orders"),

        F.round(
            F.sum("net_revenue"), 2
        ).alias("net_revenue"),

        F.round(
            F.avg("net_revenue"), 2
        ).alias("avg_order_value"),

        F.round(
            F.sum("discount_amount"), 2
        ).alias("total_discount")
    )
    .orderBy(
        F.desc("net_revenue")
    )
)

# COMMAND ----------

display(segment_performance)

# COMMAND ----------

segment_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        f"`{current_catalog}`.tpch_gold.segment_performance"
    )

# COMMAND ----------

delivery_performance = (
    gold_order_performance
    .groupBy(
        "region",
        "order_year"
    )
    .agg(
        F.countDistinct("order_id").alias("total_orders"),

        F.round(
            F.avg("avg_shipping_days"), 2
        ).alias("avg_shipping_days"),

        F.sum("late_delivery_flag").alias("late_orders"),

        F.round(
            F.avg("late_delivery_flag") * 100,
            2
        ).alias("late_delivery_rate_pct")
    )
    .orderBy(
        "order_year",
        "region"
    )
)

# COMMAND ----------

display(delivery_performance)

# COMMAND ----------

delivery_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        f"`{current_catalog}`.tpch_gold.delivery_performance"
    )

# COMMAND ----------

display(
    spark.sql(
        f"SHOW TABLES IN `{current_catalog}`.tpch_gold"
    )
)