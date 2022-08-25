import sys
from pyspark.sql import *
from pyspark.sql.functions import *
from configparser import ConfigParser
cf=ConfigParser()
cf.read(r"E:\\Drivers_important\\pass.txt")
username=cf.get("cred","usr")
password=cf.get("cred","pswd")
host=cf.get("cred","url")
mshost=cf.get("cred","msurl")
mspassword=cf.get("cred","mspas")
msusername=cf.get("cred","msusr")
spark = SparkSession.builder.master("local").appName("test").config("spark.sql.session.timeZone","EST").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
#get data from mysql
df=spark.read.format("jdbc")\
    .options(url=host,user=username,password=password,dbtable="emp",driver="com.mysql.jdbc.Driver").\
    load()
# you cal also use query="select * from emp where id>3"
df.show()

#export data to mysql--table is automatically created in mysql

df.write.mode("overwrite").format("jdbc").\
    options(url=host,user=username,password=password,dbtable="empexport1",\
            driver="com.mysql.jdbc.Driver",createTableColumnTypes="id int,name varchar(10),sal int").save()

#export all tables of mysql to mssql
alltabs="select table_name from information_schema.tables where table_schema='mysqldb'"
msdf=spark.read.format("jdbc").\
    options(url=host,user=username,password=password,query=alltabs,driver="com.mysql.jdbc.Driver").\
    load()
for i in msdf.collect():
    tab=i[0]
    df = spark.read.format("jdbc") \
        .options(url=host, user=username, password=password, dbtable=tab, driver="com.mysql.jdbc.Driver"). \
        load()
    df.show(4)
    df.write.mode("overwrite").format("jdbc").\
        options(url=mshost, user=msusername, password=mspassword, dbtable=tab, driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"). \
        save()