from pyspark.sql import window
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
spark=SparkSession.builder.master("local").enableHiveSupport().appName("test").getOrCreate()
data="E:\\Datasets\\complexxmldata.xml"
df=spark.read.format("xml").option("rowTag","catalog_item").load(data)
op1="E:\\Datasets\\output\\xml"
#u can also use all pandas command in spark
df.toPandas().to_csv(op1)
df.show()
df.printSchema()   #here u will get complex datatypes like array,struct so to convert this into
#structured format use below function
def read_nested_json(df):
    column_list = []
    for column_name in df.schema.names:
        if isinstance(df.schema[column_name].dataType, ArrayType):
            df = df.withColumn(column_name, explode(column_name).alias(column_name))
            column_list.append(column_name)
        elif isinstance(df.schema[column_name].dataType, StructType):
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)
    df = df.select(column_list)
    return df


is_all_columns_flattened = False
while not is_all_columns_flattened:
    # existing columns are flattened and appended to the schema columns
    df = read_nested_json(df)
    is_all_columns_flattened = True
    # check till all new columns are flattened
    for column_name in df.schema.names:
        if isinstance(df.schema[column_name].dataType, ArrayType):
            is_all_columns_flattened = False
        elif isinstance(df.schema[column_name].dataType, StructType):
            is_all_columns_flattened = False
cols = [re.sub('[^a-zA-Z0-9]', "", c.lower()) for c in df.columns]
ndf=df.toDF(*cols)
ndf.printSchema()
ndf.show()
#command to convert structured data into xml format
op2="E:\\Datasets\\tshirt"
ndf.write.format("xml").option("rootTag","details").option("rowTag","shirts").save(op2)
#here sometimes u get error like class not found so download txw2Runtime dependency from mavenrepository
