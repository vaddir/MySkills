# Databricks notebook source
# MAGIC %md
# MAGIC Syntax:
# MAGIC   
# MAGIC df.withColumn("New or existing column", when (Condition1, Result1)
# MAGIC .when(Condition2, Result2)
# MAGIC .when(ConditionN, ResultN)
# MAGIC .otherwise (Result))
# MAGIC
# MAGIC
# MAGIC
# MAGIC df.withColumn("New or existing column ", expr("CASE WHEN Condition1 THEN Result1"+
# MAGIC "WHEN Condition2 THEN Result2"+
# MAGIC "WHEN ConditionN THEN ResultN" +
# MAGIC "ELSE Result END"))
# MAGIC
# MAGIC To Combine multiple conditions: '&' for AND; 'I' for OR

# COMMAND ----------

student=[("Raja", "Science", 88, "P",90),
              ("Rakesh", "Maths",99, "P", 78), 
              ("Rana", "English", 28, "F", 88), 
              ("Ramesh", "Science", 45, "F", 75),
              ("Rajesh", "Maths", 38, "F",50), 
              ("Raghav", "Maths", None, "NA",70)]

Schema=["name", "Subject", "Mark", "Status", "Attendance"]
df= spark.createDataFrame(data=student, schema=Schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Update the column

# COMMAND ----------

from pyspark.sql.functions import *

df_updated= df.withColumn("Status", when(df.Mark>50, "Pass").when(df.Mark<50, "Fail").otherwise("Absent"))

display(df_updated)

# COMMAND ----------

# MAGIC %md
# MAGIC Use multiple conditions

# COMMAND ----------

df_updated= df_updated.withColumn("Grade", when( (df_updated.Mark>=80) & (df_updated.Attendance>=80), "Distinction")
                                  .when(  (df_updated.Mark>=50) & (df_updated.Attendance>=50) , "Good")
                                  .otherwise("Average"))

display(df_updated)

# COMMAND ----------

# MAGIC %md
# MAGIC
