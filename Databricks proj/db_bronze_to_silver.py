# Databricks notebook source
# MAGIC %md
# MAGIC Connect to Bronze and silver

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.window import *
import pandas as pd
import random
import os

# COMMAND ----------

# MAGIC %md
# MAGIC Parquet to Delta

# COMMAND ----------


def parquet_to_delta():
    
    for file in files_info:
       
        try:
            if os.path.exists("/dbfs/mnt/bronze/"+ file.name):
                df_delta=spark.read.option("multiline","true").parquet("dbfs:/mnt/bronze/"+ file.name)
                df_delta.write.format('delta').mode('overwrite').save('/mnt/silver/'+ file.name.split('.')[0])
                dbutils.fs.mv("dbfs:/mnt/bronze/"+file.name, "dbfs:/mnt/archive/")
            else:
                print(FileNotFoundError)
        except Exception as e:
            print(e)
            dbutils.fs.mv("dbfs:/mnt/bronze/"+file.name, "dbfs:/mnt/error/")
        

files_info = dbutils.fs.ls("dbfs:/mnt/bronze")
parquet_to_delta()




# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/silver/")

# COMMAND ----------


dept_list = ["dept1", "dept2", "dept3", "dept4","dept5"]
def random_dept():
    df=random.choice(dept_list)
    return df

# COMMAND ----------


def create_df(bpath):   
    file_details = [(file.name.split('/')[0], file.modificationTime,random_dept())]
    file_details_df = spark.createDataFrame(file_details, ["file_name", "timestamp","Department_name"])
    file_details_df = file_details_df.withColumn("date", to_date(from_unixtime(col("timestamp") / 1000)))\
    .withColumn("timestamp",from_unixtime(col("timestamp") / 1000).cast("timestamp") )\
    .withColumn("Department_name",col("Department_name"))
    display(file_details_df)
    return file_details_df

 #date_format(from_unixtime(col("timestamp") / 1000).cast("timestamp"), 'HH:mm:ss')   

files_info = dbutils.fs.ls("dbfs:/mnt/silver")


for file in files_info:
    file=create_df("/dbfs/mnt/silver/"+ file.name)
    file.write.format('delta').option("mergeSchema", "true").mode("append").saveAsTable('file2')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from file2;
# MAGIC
# MAGIC
# MAGIC
# MAGIC --SELECT * FROM DELTA.`/dbfs/mnt/delta/file2`;
# MAGIC --drop table file1;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Load file1 data to Lookup Table
