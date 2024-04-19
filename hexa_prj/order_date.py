# Databricks notebook source
# MAGIC %run "/Workspace/Users/aadityabhure61@gmail.com/hexa_prj/includes"
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/Users/aadityabhure61@gmail.com/hexa_prj/functions"

# COMMAND ----------

df = spark.read.option("inferSchema", "True").csv("dbfs:/FileStore/Hexa_prj/order_dates.csv", header = True)

# COMMAND ----------

df.show()

# COMMAND ----------

df_order_date = df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

df_order_date.display()

# COMMAND ----------

df_order_date.write.mode("overwrite").saveAsTable("adityas.hexa_prj.order_date")
