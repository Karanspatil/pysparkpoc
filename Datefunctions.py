from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
#set timezone
spark = SparkSession.builder.master("local").appName("test").config("spark.sql.session.timeZone","IST").\
    getOrCreate()
data="E:\\Datasets\\donations.csv"
df=spark.read.format('csv').options(header='true',inferSchema='true').load(data)
#spark default date format==yyyy-MM-dd
#convert string column to date column using to_date()
res=df.withColumn("today",current_date()).\
    withColumn("dt",to_date(col("dt"),"d-M-yyyy"))

#convert date to specified format
res1=res.withColumn("dt",date_format(col("dt"),"yyyy-MM-dd"))
res1.printSchema()
res1.show()

#calculate difference between 2 dates and add/substract days from date
res1=res.withColumn("daydiff",datediff(col("today"),col("dt"))).\
    withColumn("monthdiff",months_between(col("today"),col("dt"))).\
    withColumn("newdate1",date_add(col("today"),20)).\
    withColumn("newdate2",date_sub(col("today"),10)).\
    withColumn("monthadd",add_months(col("today"),2))
res1.show()

res2=res.withColumn("lstday",last_day(col("today"))).\
    withColumn("nextFri",next_day(col("today"),'Fri')).\
    withColumn("yyy",year(col("today"))).\
    withColumn("dayofm",dayofmonth(col("today"))).\
    withColumn("dayofwk",dayofweek(col("today"))).\
    withColumn("dayofyr",dayofyear(col("today"))).\
    withColumn("wkofyear",weekofyear(col("today"))).\
    withColumn("quarter",quarter(col("today"))).\
    withColumn("truncate",date_trunc("month",col("today").cast(DateType())))
res2.show()

#timestamp functions
#unix_timestamp()--seconds since epoch(1971)
#to_timestamp() or from_unixtime()--converts epoch time(seconds) to date
#to_utc_timestamp()---converts 1 timezone to another timezone ex..converts indian time to usa time
res3=res.withColumn("timestmp",current_timestamp()).\
    withColumn("epochSeconds",unix_timestamp()).\
    withColumn("epoch",unix_timestamp(col("today"))).\
    withColumn("newdate",to_timestamp(col("epochSeconds"))).\
    withColumn("ustime",to_utc_timestamp(col("timestmp"),"EST"))
res3.show()

#display last friday date of month
res4=res.withColumn("fridate",next_day((last_day(col("today"))-7),'Fri'))
res4.show()
