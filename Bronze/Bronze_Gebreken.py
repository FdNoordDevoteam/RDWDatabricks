# Databricks notebook source
from datetime import date
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *

# COMMAND ----------

datetoday = date.today().strftime("%Y-%m-%d")
firstdate = "2023-03-24"
yesterday = "2023-03-29"

# COMMAND ----------

# %sql
# CREATE TABLE gebreken_table_delta_b
# ()
# LOCATION '/mnt/iotdata/Gebreken/table/tbl_gebreken_Bd'

# COMMAND ----------

# gebreken_bronze = spark.read.json(f'/mnt/iotdata/Gebreken/{firstdate}/*.json')
# gebreken_bronze= gebreken_bronze.withColumn("Start_Date", lit(firstdate).cast(DateType())).withColumn("End_Date",lit(None).cast(DateType()))
# gebreken_bronze.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save("/mnt/iotdata/Gebreken/table/tbl_gebreken_Bd")
tbl_gebreken_Bd = DeltaTable.forPath(spark, "/mnt/iotdata/Gebreken/table/tbl_gebreken_Bd")
gebreken_B_DF = tbl_gebreken_Bd.toDF()
display(gebreken_B_DF)

# COMMAND ----------

gebreken_bronze_dump = spark.read.json(f'/mnt/iotdata/Gebreken/{yesterday}/*.json')
display(gebreken_bronze_dump)

# COMMAND ----------

# data = [(None, '006','TEST','2023-03-29')]
# df = spark.createDataFrame(data, schema=gebreken_bronze_dump.schema)
# gebreken_bronze_dump = gebreken_bronze_dump.union(df)


# COMMAND ----------

# gebreken_bronze_dump = gebreken_bronze_dump.withColumn("gebrek_omschrijving", when(gebreken_bronze_dump.gebrek_identificatie == "010", " TESTTTTTTTTT").otherwise(gebreken_bronze_dump.gebrek_omschrijving))

# COMMAND ----------

# for i in range(len(gebreken_B_DF.columns )-2):
#     print("gebreken_B_DF."+gebreken_B_DF.columns[i]+f'.alias("target_{gebreken_B_DF.columns[i]}"),')

# COMMAND ----------

for i in range(len(gebreken_B_DF.columns)-2):
    gebreken_B_DF = gebreken_B_DF.withColumnRenamed(gebreken_B_DF.columns[i], "target_"+gebreken_B_DF.columns[i])

gebreken_B_DF = gebreken_B_DF.drop(gebreken_B_DF[-1]).drop(gebreken_B_DF[-2])
display(gebreken_B_DF)

# COMMAND ----------

joinDF = gebreken_bronze_dump.join(gebreken_B_DF,(gebreken_bronze_dump.gebrek_identificatie==gebreken_B_DF.target_gebrek_identificatie), "leftouter").select(gebreken_bronze_dump["*"], gebreken_B_DF["*"])
display(joinDF)

# COMMAND ----------

# strJoinDF = ""

# for i in range(int(len(joinDF.columns )/2)):
#     if i != (int(len(joinDF.columns)/2)-1):
#         komma = ","
#     else:
#         komma = ""
#     strJoinDF += "joinDF."+joinDF.columns[i]+komma

# print(strJoinDF)

# strJoinDFT = ""
# for i in range(int(len(joinDF.columns )/2),len(joinDF.columns )):
#     if i != (int(len(joinDF.columns))-1):
#         komma = ","
#     else:
#         komma = ""
#     strJoinDFT += "joinDF."+joinDF.columns[i]+komma
# print(strJoinDFT)

# COMMAND ----------

array_source_columns = array(joinDF.columns[0:int(len(joinDF.columns)/2)])
array_target_columns = array(joinDF.columns[int(len(joinDF.columns)/2):])


# COMMAND ----------

filterDF = joinDF.filter(xxhash64(array_source_columns
)!=xxhash64(array_target_columns))
display(filterDF)

# COMMAND ----------

mergeDF = filterDF.withColumn("MERGEKEY", filterDF.gebrek_identificatie)
display(mergeDF)

# COMMAND ----------

dummyDF = filterDF.filter("target_gebrek_identificatie is not null").withColumn("MERGEKEY", lit(None))
display(dummyDF)

# COMMAND ----------

scdDF = mergeDF.union(dummyDF)
display(scdDF)

# COMMAND ----------

dict_delta = dict()
for column_DF in scdDF.columns[:int(len(scdDF.columns)/2)]:
    dict_delta[column_DF] = "source."+column_DF

dict_delta["Start_Date"] = "current_date"
dict_delta["End_Date"] = "null"
display(dict_delta)

# COMMAND ----------

tbl_gebreken_Bd.alias("target").merge(
source = scdDF.alias("source"),
condition = "target.gebrek_identificatie = source.MERGEKEY and target.End_Date is null"
).whenMatchedUpdate(set = 
                   {
                       "End_Date" : "current_date"
                   }
).whenNotMatchedInsert(values = dict_delta).execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * FROM gebreken_table_delta_b
# MAGIC order by gebrek_identificatie
