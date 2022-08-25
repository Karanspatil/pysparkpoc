#EMR
#read emp,asl data from cassandra ,process in spark(apply joins) and again write this data in cassendra(table must create in advance) and hive also
import os
import sys
import findspark
findspark.init()
os.environ["JAVA_HOME"]='/usr/lib/jvm/java'
os.environ["SPARK_HOME"]='/usr/lib/spark'
from pyspark.sql import *
from pyspark.sql.functions import *
spark=SparkSession.builder.master("local").enableHiveSupport().appName("test").getOrCreate()
df1=spark.read.format("org.apache.spark.sql.cassandra").option("table","asl").option("keyspace","cassdb").load()
df1.show()
df2=spark.read.format("org.apache.spark.sql.cassandra").option("table","emp").option("keyspace","cassdb").load()
df2.show()
join=df1.join(df2,df1.name==df2.name,"inner").drop(df2.name)
join.show()

#write this data in cassandra
join.write.mode("append").format("org.apache.spark.sql.cassandra").option("table","jointab").option("keyspace","cassdb").save()

#write this data in hive
join.write.mode("overwrite").format("hive").saveAsTable("empasljoin")
