# Databricks notebook source
display(spark.sql("SHOW CATALOGS"))

# COMMAND ----------

display(spark.sql("SHOW SCHEMAS IN samples"))

# COMMAND ----------

display(spark.sql("SHOW TABLES IN samples.tpch"))

# COMMAND ----------

orders_df = spark.read.table("samples.tpch.orders")
display(orders_df.limit(10))

# COMMAND ----------

lineitem_df = spark.read.table("samples.tpch.lineitem")
display(lineitem_df.limit(10))

# COMMAND ----------

customer_df = spark.read.table("samples.tpch.customer")
display(customer_df.limit(10))