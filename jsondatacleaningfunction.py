from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\Datasets\\world_bank.json"
df=spark.read.format("json").option("multiLine","true").load(data)
df.printSchema()
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
##ref: https://github.com/maroovi/aws_etl/blob/23d5af070f6c605852ff91fdf0f28423548f59e5/glue.py
#https://github.com/krish-17/CSCI5408_Assignments/blob/bf72c77b14ea6320c77accfe306e944fc6a3d4f5/lab_6/lab6.py
