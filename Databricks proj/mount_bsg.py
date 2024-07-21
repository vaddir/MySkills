# Databricks notebook source
from pyspark.dbutils import *
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

config={
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": "6ab76798-a127-45bd-a71e-96668397b832",
    "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="projScope",key="appsecret"),
    "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/7016853f-19fe-467e-bfcc-9dbe8b2ca8e1/oauth2/token"
  }

mount="/mnt/sourcedata/"
mount1="/mnt/bronze/"
mount2="/mnt/silver/"
mount3="/mnt/gold/"
mount4="/mnt/error/"
mount5="/mnt/archive/"
mount6="/mnt/delta/"

def is_mounted(mount_point):
    mounts = dbutils.fs.mounts()
    for mount in mounts:
        if mount.mountPoint == mount_point:
            return True
    return False

if not is_mounted(mount):
  dbutils.fs.mount(
  source="abfss://sourcedata@strgmylearning.dfs.core.windows.net/",
  mount_point=mount,
  extra_configs=config
  )
else:
  print("Mounted already to source")


if not is_mounted(mount1):
  dbutils.fs.mount(
  source="abfss://bronze@strgmylearning.dfs.core.windows.net/",
  mount_point=mount1,
  extra_configs=config
  )
else:
  print("Mounted already to bronze")

if not is_mounted(mount2):
  dbutils.fs.mount(
  source="abfss://silver@strgmylearning.dfs.core.windows.net/",
  mount_point=mount2,
  extra_configs=config
  )
else:
  print("Mounted already to silver")

if not is_mounted(mount3):
  dbutils.fs.mount(
  source="abfss://gold@strgmylearning.dfs.core.windows.net/",
  mount_point=mount3,
  extra_configs=config
  )
else:
  print("Mounted already to gold")



if not is_mounted(mount4):
  dbutils.fs.mount(
  source="abfss://error@strgmylearning.dfs.core.windows.net/",
  mount_point=mount4,
  extra_configs=config
  )
else:
  print("Mounted already to error")


if not is_mounted(mount5):
  dbutils.fs.mount(
  source="abfss://archive@strgmylearning.dfs.core.windows.net/",
  mount_point=mount5,
  extra_configs=config
  )
else:
  print("Mounted already to archive")



if not is_mounted(mount6):
  dbutils.fs.mount(
  source="abfss://archive@strgmylearning.dfs.core.windows.net/",
  mount_point=mount6,
  extra_configs=config
  )
else:
  print("Mounted already to archive")

# COMMAND ----------


