# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS LOOKUP_TABLE (
# MAGIC file_name STRING,
# MAGIC BtoSdate DATE,
# MAGIC BtoSts TIMESTAMP,
# MAGIC StoGdate DATE,
# MAGIC StoGts TIMESTAMP,
# MAGIC processingStage STRING
# MAGIC )
# MAGIC USING delta

# COMMAND ----------

from pyspark.dbutils import *
dbutils.widgets.text("Input",'')
try:
    if(dbutils.widgets.get("Input")=="bronzetosilver"):
        spark.sql("INSERT INTO LOOKUP_TABLE (file_name, BtoSdate,BtoSts,processingStage) SELECT file_name,date,timestamp,'Pending' from file2;")
    elif(dbutils.widgets.get("Input")=="silvertogold"):
        spark.sql("INSERT INTO LOOKUP_TABLE (file_name, StoGdate,StoGts,processingStage) SELECT file_name,date,timestamp,'Success' from gold_table")
except Exception as e:
    print(e)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lookup_table;
