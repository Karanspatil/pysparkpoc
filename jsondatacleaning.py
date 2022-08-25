from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local").appName("test").getOrCreate()
data="E:\\Datasets\\world_bank.json"
df=spark.read.format("json").options(inferSchema='true').load(data)
df.printSchema()
'''
root
 |-- _id: struct (nullable = true)
 |    |-- $oid: string (nullable = true)
 |-- approvalfy: string (nullable = true)
 |-- board_approval_month: string (nullable = true)
 |-- boardapprovaldate: string (nullable = true)
 |-- borrower: string (nullable = true)
 |-- closingdate: string (nullable = true)
 |-- country_namecode: string (nullable = true)
 |-- countrycode: string (nullable = true)
 |-- countryname: string (nullable = true)
 |-- countryshortname: string (nullable = true)
 |-- docty: string (nullable = true)
 |-- envassesmentcategorycode: string (nullable = true)
 |-- grantamt: long (nullable = true)
 |-- ibrdcommamt: long (nullable = true)
 |-- id: string (nullable = true)
 |-- idacommamt: long (nullable = true)
 |-- impagency: string (nullable = true)
 |-- lendinginstr: string (nullable = true)
 |-- lendinginstrtype: string (nullable = true)
 |-- lendprojectcost: long (nullable = true)
 |-- majorsector_percent: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- Name: string (nullable = true)
 |    |    |-- Percent: long (nullable = true)
 |-- mjsector_namecode: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- code: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |-- mjtheme: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- mjtheme_namecode: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- code: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |-- mjthemecode: string (nullable = true)
 |-- prodline: string (nullable = true)
 |-- prodlinetext: string (nullable = true)
 |-- productlinetype: string (nullable = true)
 |-- project_abstract: struct (nullable = true)
 |    |-- cdata: string (nullable = true)
 |-- project_name: string (nullable = true)
 |-- projectdocs: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- DocDate: string (nullable = true)
 |    |    |-- DocType: string (nullable = true)
 |    |    |-- DocTypeDesc: string (nullable = true)
 |    |    |-- DocURL: string (nullable = true)
 |    |    |-- EntityID: string (nullable = true)
 |-- projectfinancialtype: string (nullable = true)
 |-- projectstatusdisplay: string (nullable = true)
 |-- regionname: string (nullable = true)
 |-- sector: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- Name: string (nullable = true)
 |-- sector1: struct (nullable = true)
 |    |-- Name: string (nullable = true)
 |    |-- Percent: long (nullable = true)
 |-- sector2: struct (nullable = true)
 |    |-- Name: string (nullable = true)
 |    |-- Percent: long (nullable = true)
 |-- sector3: struct (nullable = true)
 |    |-- Name: string (nullable = true)
 |    |-- Percent: long (nullable = true)
 |-- sector4: struct (nullable = true)
 |    |-- Name: string (nullable = true)
 |    |-- Percent: long (nullable = true)
 |-- sector_namecode: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- code: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |-- sectorcode: string (nullable = true)
 |-- source: string (nullable = true)
 |-- status: string (nullable = true)
 |-- supplementprojectflg: string (nullable = true)
 |-- theme1: struct (nullable = true)
 |    |-- Name: string (nullable = true)
 |    |-- Percent: long (nullable = true)
 |-- theme_namecode: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- code: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |-- themecode: string (nullable = true)
 |-- totalamt: long (nullable = true)
 |-- totalcommamt: long (nullable = true)
 |-- url: string (nullable = true)
'''
#exlplode---removes Array so that Struct goes up
res=df.withColumnRenamed("_id","id").\
    withColumn("majorsector_percent",explode(col("majorsector_percent"))).\
    withColumn("majorsector_percent_name",col("majorsector_percent.Name")).\
    withColumn("majorsector_percent",col("majorsector_percent.Percent")).\
    withColumn("mjsector_namecode",explode(col("mjsector_namecode"))).\
    withColumn("mjsector_namecode_code",col("mjsector_namecode.code")).\
    withColumn("mjsector_namecode_name",col("mjsector_namecode.name")).\
    withColumn("mjtheme",explode(col("mjtheme"))).\
    withColumn("mjtheme_namecode",col("mjtheme_namecode")).\
    withColumn("mjtheme_namecode_code",col("mjtheme_namecode.code")).\
    withColumn("mjtheme_namecode_name",col("mjtheme_namecode.name")).\
    withColumn("project_abstract",col("project_abstract")).\
    withColumn("projectdocs",explode(col("projectdocs"))).\
    withColumn("projectdocs_docdate",col("projectdocs.DocDate")).\
    withColumn("projectdocs_doctype",col("projectdocs.DocType")).\
    withColumn("projectdocs_doctypedesc",col("projectdocs.DocTypeDesc")).\
    withColumn("projectdocs_docurl",col("projectdocs.DocURL")).\
    withColumn("projectdocs_entityid",col("projectdocs.EntityID")).\
    withColumn("sector",explode(col("sector"))).\
    withColumn("sector_name",col("sector.Name")).\
    withColumn("sector1_name",col("sector1.Name")).\
    withColumn("sector1_percent",col("sector1.Percent")).\
    withColumn("sector2_name",col("sector2.Name")).\
    withColumn("sector2_percent",col("sector2.Percent")).\
    withColumn("sector3_name",col("sector3.Name")).\
    withColumn("sector3_percent",col("sector3.Percent")).\
    withColumn("sector4_name",col("sector4.Name")).\
    withColumn("sector4_percent",col("sector4.Percent")).\
    withColumn("sector_namecode",explode("sector_namecode")).\
    withColumn("sector_namecode_code",col("sector_namecode.code")).\
    withColumn("sector_namecode_name",col("sector_namecode.name")).\
    withColumn("theme1_name",col("theme1.Name")).\
    withColumn("theme1_percent",col("theme1.Percent")).\
    withColumn("theme_namecode",explode("theme_namecode")).\
    withColumn("theme_namecode_code",col("theme_namecode.code")).\
    withColumn("theme_namecode_name",col("theme_namecode.name")).\
    drop("mjsector_namecode","mjtheme_namecode","project_abstract","projectdocs","sector","sector1"\
         ,"sector2","sector3","sector4","sector_namecode","theme1","theme_namecode"\
         ,"mjtheme_namecode_code","mjtheme_namecode_name")

res.printSchema()
res.show(6)