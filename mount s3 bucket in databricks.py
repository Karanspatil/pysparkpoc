# Databricks notebook source
#mount s3 in datbricks
import urllib
ACCESS_KEY = "xxxxxxxxxxxxxxxx"
SECRET_KEY = "xxxxxxxxxx/xxxxxxxxx".replace("/", "%2F")
ENCODED_SECRET_KEY = urllib.parse.quote(SECRET_KEY," ")
AWS_BUCKET_NAME = "karan2020/input/employ/"
MOUNT_NAME = "mys3data1"
dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

display(dbutils.fs.ls("/mnt/mys3data"))
data="dbfs:/mnt/mys3data1/emp.csv"
df=spark.read.format("csv").options(header='true',inferSchema='true').load(data)
df.show()

# COMMAND ----------

display(dbutils.fs.ls("/mnt/mys3data1"))

# COMMAND ----------

data="dbfs:/mnt/mys3data1/emp.csv"
df=spark.read.format("csv").options(header='true',inferSchema='true').load(data)
df.show()

# COMMAND ----------


