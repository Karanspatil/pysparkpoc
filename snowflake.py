from pyspark.sql import window
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark=SparkSession.builder.master("local").enableHiveSupport().appName("test").getOrCreate()
sfoptions={"sfurl":"yb65986.ap-southeast-1.snowflakecomputing.com",
           "sfuser":"xxx",
           "sfpassword":"xxxxx",
           "sfDatabase":"KARANDB",
           "sfSchema":"PUBLIC",
           "sfWarehouse":"SPARKNEW" }
snowflake_source_name="net.snowflake.spark.snowflake"
##read data from snowflake
df=spark.read.format(snowflake_source_name).\
    options(**sfoptions).option("query","select * from banktab").load()
df.show()

##write data to snowflake
data="E:\\Datasets\\asl.csv"
df=spark.read.format("csv").options(header="true",inferSchema='true').load(data)
df.write.format(snowflake_source_name).options(**sfoptions).option("dbtable","aslnew").save()

###read data from s3 in snowflake by staging file ( here s3data is stage name)
#in this stage multiple files are available
copy into aslnew
     from @s3data files = ('asl.csv')
     file_format = (format_name=asalcsv);

###read data from s3 in snowflake without staging the files
copy into aslnew
     from s3://karan2020/input/employ/asl.csv
     credentials=(aws_key_id='xxxxxxxxx', aws_secret_key='xxxxxxxxxx')
     file_format=(format_name=asalcsv);

