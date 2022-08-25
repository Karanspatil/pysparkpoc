from pyspark.sql import *
from pyspark.sql.functions import *
import re
spark = SparkSession.builder.master("local").appName("test").getOrCreate()
data="E:\\datecleaning.txt"
df=spark.read.format("csv").options(header='true',inferSchema='true').load(data)
#removing special characters from column names
cols=[re.sub('[^a-zA-Z0-9]','',i) for i in df.columns]
res=df.toDF(*cols)
res.show()
#creating user defined function for converting multiple date formats to single(yyyy-MM-dd)
def modifydt(col,frmts=("MM-dd-yyyy","yyyy-MM-dd","dd-MMM-yyyy","ddMMMMyyyy","MMM/yyyy/dd")):
    return coalesce(*[to_date(col,x) for x in frmts])
res1=res.withColumn("bbd",modifydt(col("birthdob")))
res1.show()