from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
spark = SparkSession.builder.master("local").appName("test").getOrCreate()
data="E:\\Datasets\\us-500.csv"
df=spark.read.format('csv').options(header='true',inferSchema='true').load(data)
res=df.withColumnRenamed("zip","sal").drop("address","phone1","phone2","email","web")
#calculate highest salary employ in each country
win=Window.partitionBy(col("county")).orderBy(col("sal").desc())
res1=res.withColumn("rnk",rank().over(win)).\
    withColumn("dnsrnk",dense_rank().over(win)).\
    withColumn("rowno",row_number().over(win)).\
    where(col('dnsrnk')==1)
res1.show()

#by using sql friendly
res1.createOrReplaceTempView("tab")
result=spark.sql('''select * from
                 (select e.*,dense_rank()over(partition by county order by sal desc) as rn from tab e)
                 where rn=1''')
result.show()
#percent rank and ntile-----using ntile function form group(bucket) of richer people(bckt=1)
# medium(bckt=2),and poor(bckt=3)
res1=res.withColumn("prnk",percent_rank().over(Window.orderBy(col("sal").desc()))).\
    withColumn("bckt",ntile(3).over(Window.orderBy(col("sal").desc())))
res1.show(70)

#find difference of salary between  current employ and next employ
res1=res.withColumn("next",lead(col("sal")).over(Window.orderBy(col("sal")))).\
    withColumn("diff",col("next")-col('sal'))
res1.show()

#first and last value
win1=Window.orderBy(col("sal")).rowsBetween(Window.currentRow,Window.unboundedFollowing)
res2=res.withColumn("fvalue",first(col("sal")).over(Window.orderBy(col("sal")))).\
    withColumn("lvalue",last(col("sal")).over(win1))
res2.show()