# Databricks notebook source
# MAGIC %md
# MAGIC Union is used to combine two dataframes. To perform union the schema of the two dataframes should be same.
# MAGIC From spark version 2.0.0 Union includes duplicate rows, to remove duplicates use dropDuplicate()
# MAGIC _Syntax:_
# MAGIC
# MAGIC df_union= df1.union(df2)
# MAGIC
# MAGIC

# COMMAND ----------

employee_data = [(100, "Stephen", "1999","166","M", 2000), 
                 (290, "Philipp", "2002","206", "M", 8900), 
                 (300, "John", "2018","166","",6669)]

employee_schema = ["employee_id", "name", "doj", "employee dept_id", "gender", "salary"]

df1=spark.createDataFrame(data=employee_data,schema=employee_schema)

display(df1)

# COMMAND ----------

employee_data = [(300, "John", "2018","166","",6669), 
                 (400, "Nancy", "2908", "400", "F", 1000), 
                 (500, "Rosy", "2014","500","M", 5066)]

employee_schema = ["employee_1d", "name","doj", "employee_dept 1d", "gender", "salary"]

df2=spark.createDataFrame(data=employee_data,schema=employee_schema)

display(df2)

# COMMAND ----------

df_union= df1.union(df2)

display(df_union)

# COMMAND ----------

display(df_union.dropDuplicates())
