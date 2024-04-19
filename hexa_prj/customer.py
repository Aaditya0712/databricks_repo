# Databricks notebook source
# MAGIC %run "/Workspace/Users/aadityabhure61@gmail.com/hexa_prj/includes"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/aadityabhure61@gmail.com/hexa_prj/functions"

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/Hexa_prj/customers.json", multiLine= True)

# COMMAND ----------

df_cust = df.withColumnRenamed("customer_id", "ID").withColumnRenamed("customer_name", "Name").withColumnRenamed("customer_email", "Email").withColumn("Address", concat("customer_city", lit(", "), "customer_state")).drop("customer_state").drop("customer_city")

# COMMAND ----------

df_cust.display()

# COMMAND ----------

df_cust.select("ID",  col("Name"),df_cust["Email"], df_cust.Address).show()

# COMMAND ----------

df_cust.write.option("mergeSchema", True).mode("overwrite").saveAsTable("adityas.hexa_prj.Customer")
