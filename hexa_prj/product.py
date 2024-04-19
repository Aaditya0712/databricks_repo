# Databricks notebook source
# MAGIC %run "/Workspace/Users/aadityabhure61@gmail.com/hexa_prj/includes"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/aadityabhure61@gmail.com/hexa_prj/functions"

# COMMAND ----------

df= spark.read.json("dbfs:/FileStore/Hexa_prj/products.json", multiLine =True)

# COMMAND ----------

df_products = df.withColumnRenamed("product_category", "Product_Category")\
    .withColumnRenamed("product_id", "Product_Id")\
        .withColumnRenamed("product_name", "Product_Name")

# COMMAND ----------

df_products.display()

# COMMAND ----------

df_products.write.option("mergeSchema", True).mode("overwrite").saveAsTable("adityas.hexa_prj.Products")
