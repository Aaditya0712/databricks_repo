# Databricks notebook source
# MAGIC %run "/Workspace/Users/aadityabhure61@gmail.com/hexa_prj/includes"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/aadityabhure61@gmail.com/hexa_prj/functions"
# MAGIC

# COMMAND ----------

dbutils.widgets.text("Data_source", " ")
var= dbutils.widgets.get("Data_source")

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/Hexa_prj/sales.csv", header= True, inferSchema= True)

# COMMAND ----------

df.columns

# COMMAND ----------

new_col = ['Order_Id','Customer_Id','Transaction_Id','Product_Id','Quantity','Discount','Total','Order_Date']

# COMMAND ----------

df_sales = df.toDF(*new_col)

# COMMAND ----------

df_sales = df_sales.withColumn("Data_source", lit("var"))

# COMMAND ----------

df_sales.show()

# COMMAND ----------

df_sales.dropDuplicates(["Order_Id"]).dropna().filter("Order_Id != 'None'")

# COMMAND ----------

grp = df_sales.groupBy("Customer_Id")

# COMMAND ----------

df_sales.write.option("mergeSchema", True).mode("overWrite").saveAsTable("adityas.hexa_prj.Sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from adityas.hexa_prj.Sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select Customer_Id, count(1) from adityas.hexa_prj.Sales group by Customer_Id

# COMMAND ----------


