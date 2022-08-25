#EMR
import os
import sys
import findspark
import re
findspark.init()
os.environ["JAVA_HOME"]='/usr/lib/jvm/java'
os.environ["SPARK_HOME"]='/usr/lib/spark'
from pyspark.sql import *
from pyspark.sql.functions import *
spark=SparkSession.builder.master("local").appName("test").enableHiveSupport().getOrCreate()
tab=sys.argv[1]
s3path=sys.argv[2]
host="jdbc:mysql://karandb.cnhtjdwvatxj.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
#read data from mysql
res=spark.read.format("jdbc").options(url=host,user='myuser',password='mypassword',dbtable=tab,driver='com.mysql.jdbc.Driver').load()
res1=res.na.fill(0)
res1.show(5)
#load this data in hive as a managed table
res1.write.mode("overwrite").format("hive").saveAsTable(tab)
#load this data in s3
res1.write.format('csv').option('header','true').save(s3path)

#load this data in hive as external table and do partitioning on state column
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
res1.write.mode("overwrite").format("hive").partitionBy("state").option("path",s3path).saveAsTable(tab)
