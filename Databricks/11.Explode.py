# Databricks notebook source
Ar_app=[
    ('Manish',['TV','Refrigerator','Oven','AC']),
    ('Raghav',['AC','Washing Machine',None]),
    ('Ram',['Mixer','TV']),
    ('Ramesh',[None])
]
df=spark.createDataFrame(data=Ar_app,schema=['Name','Appliances'])
df.printSchema()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Create DF with map type
# MAGIC

# COMMAND ----------

Ar_app=[
    ('Manish',{'TV':'LG','Refrigerator':'Samsung','Oven':'Phillips','AC':'Voltas'}),
    ('Raghav',{'AC':'Samsung','Washing Machine':'LG'}),
    ('Ram',{'Mixer':'Preethi','TV':''}),
    ('Ramesh',None)
]

df_brand=spark.createDataFrame(data=Ar_app,schema=['Name','brand'])
df_brand.printSchema()
display(df_brand)

# COMMAND ----------

# MAGIC %md
# MAGIC Explode array DF

# COMMAND ----------

from pyspark.sql.functions import *

df_e1=df.select(df.Name,explode(df.Appliances))

df.printSchema()
display(df)

df_e1.printSchema()
display(df_e1)

# COMMAND ----------

# MAGIC %md
# MAGIC Explode map DF

# COMMAND ----------

df_e2=df_brand.select(df_brand.Name,explode(df_brand.brand))

df_brand.printSchema()
display(df_brand)

df_e2.printSchema()
display(df_e2)

# COMMAND ----------

# MAGIC %md
# MAGIC Explode Outer: includes NULL values too
# MAGIC

# COMMAND ----------

display(df.select(df.Name,explode_outer(df.Appliances)))

display(df_brand.select(df_brand.Name,explode_outer(df_brand.brand)))

# COMMAND ----------

# MAGIC %md
# MAGIC Position Explode: column is created for position of each element

# COMMAND ----------

display(df.select(df.Name,posexplode(df.Appliances)))

display(df_brand.select(df_brand.Name,posexplode(df_brand.brand)))

# COMMAND ----------

# MAGIC %md
# MAGIC Position Explode outer: column is created for position of each element and includes null values also

# COMMAND ----------

display(df.select(df.Name,posexplode_outer(df.Appliances)))

display(df_brand.select(df_brand.Name,posexplode_outer(df_brand.brand)))
