from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.streaming import *
import re
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
ssc=StreamingContext(spark.sparkContext,10)
#host="ec2-43-205-213-29.ap-south-1.compute.amazonaws.com"
dsm=ssc.socketTextStream('43.205.213.29',2222)
#create spark subsession from existing sparksession to convert dstream to rdd using foreeachRDD
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

# DataFrame operations inside your streaming program
def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession to convert dstream to rdd
        spark = getSparkSessionInstance(rdd.context.getConf())
        cols=["name",'age','sal']
        df=rdd.map(lambda x:x.split(',')).toDF(cols)
        df.show()
        host="jdbc:mysql://karandb.cnhtjdwvatxj.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
        df.write.mode("append").format("jdbc").option("url",host).option("user","myuser").\
           option("password","mypassword").option("driver","com.mysql.jdbc.Driver").option("dbtable","stream8").save()
    except:
        pass
dsm.foreachRDD(process)

#start the computation
ssc.start()
ssc.awaitTermination()