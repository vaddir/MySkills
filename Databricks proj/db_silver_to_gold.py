# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.window import *
import pandas as pd
import random
import os

# COMMAND ----------

# MAGIC %md
# MAGIC silver to gold
# MAGIC

# COMMAND ----------


def silver_to_gold():
    
    for file in files_info:
       
        try:
            if os.path.exists("/dbfs/mnt/silver/"+file.name):
                print(file.name)
                dbutils.fs.cp("dbfs:/mnt/silver/"+file.name, "dbfs:/mnt/gold/"+file.name,recurse=True)
                dbutils.fs.mv("dbfs:/mnt/silver/"+file.name, "dbfs:/mnt/archive/"+file.name,recurse=True)
            else:
                print(FileNotFoundError)
        except Exception as e:
            print(e)
            dbutils.fs.mv("dbfs:/mnt/silver/"+file.name, "dbfs:/mnt/error/",recurse=True)
        

files_info = dbutils.fs.ls("dbfs:/mnt/silver/")
silver_to_gold()




# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/silver/")
files_info = dbutils.fs.ls("dbfs:/mnt/silver/")
for file in files_info:
    print("dbfs:/mnt/silver/"+file.name)
    if os.path.exists("/dbfs/mnt/silver/"+file.name):
        print(file.name)



# COMMAND ----------


def create_df(bpath):
    
    file_details = [(file.name, file.modificationTime)]
    file_details_df = spark.createDataFrame(file_details, ["file_name", "timestamp"])
    file_details_df = file_details_df.withColumn("date", to_date(from_unixtime(col("timestamp") / 1000)))\
    .withColumn("timestamp", date_format(from_unixtime(col("timestamp") / 1000).cast("timestamp"), 'HH:mm:ss'))
    display(file_details_df)
    return file_details_df
    

files_info = dbutils.fs.ls("dbfs:/mnt/gold")


for file in files_info:
    file=create_df("/dbfs/mnt/gold/"+ file.name)
    file.write.format('delta').option("mergeSchema", "true").mode("append").saveAsTable('final_table')


# COMMAND ----------


def create_df(bpath):
    file_details = [(file.name.split('/')[0],file.modificationTime)]
    file_details_df = spark.createDataFrame(file_details, ["file_name", "timestamp"])
    file_details_df = file_details_df.withColumn("date", to_date(from_unixtime(col("timestamp") / 1000))).withColumn("timestamp",from_unixtime(col("timestamp") / 1000).cast("timestamp"))
    display(file_details_df)
    return file_details_df

 #date_format(from_unixtime(col("timestamp") / 1000).cast("timestamp"), 'HH:mm:ss')   

files_info = dbutils.fs.ls("dbfs:/mnt/gold")

for file in files_info:
    file=create_df("/dbfs/mnt/gold/"+ file.name)
    file.write.format('delta').option("mergeSchema", "true").mode("append").saveAsTable('gold_table')

# COMMAND ----------

df1 = spark.read.format("delta").table("file1").selectExpr("id","file_name","date as entry_date")
display(df1)
df2 = spark.read.format("delta").table("file2").selectExpr("file_name","department_name","Date as dept_assign_date")
display(df2)
joined_df = df1.join(df2,df1.file_name==df2.file_name).select(df1["*"], df2.department_name, df2.dept_assign_date)

# Insert the joined data into Final_table
joined_df.write.format("delta").mode("append").save("/dbfs/mnt/delta/final_table")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta. `/dbfs/mnt/delta/final_table/`

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from gold_table;
# MAGIC --drop table gold_table;

# COMMAND ----------

# MAGIC %md
# MAGIC Modifications --> try to create single delta file
# MAGIC                 
