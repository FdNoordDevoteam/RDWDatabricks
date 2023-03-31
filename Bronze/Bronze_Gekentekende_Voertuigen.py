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

file_name = "Gekentekende_Voertuigen"
primary_key = "kenteken"
target_pm = "target_"+primary_key
table_name = "tbl_"+file_name+"_Bd"

# COMMAND ----------

# file_bronze = spark.read.json(f'/mnt/iotdata/{file_name}/{firstdate}/*.json')
# file_bronze = file_bronze.withColumn("Start_Date", lit(firstdate).cast(DateType())).withColumn("End_Date",lit(None).cast(DateType()))
# file_bronze.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(f"/mnt/iotdata/{file_name}/table/tbl_{file_name}_Bd")
tbl_file_Bd = DeltaTable.forPath(spark, f"/mnt/iotdata/{file_name}/table/tbl_{file_name}_Bd")
file_B_DF = tbl_file_Bd.toDF()
display(file_B_DF)


# COMMAND ----------

# MAGIC %sql
# MAGIC Create Table gekentekende_voertuigen_delta_b
# MAGIC ()
# MAGIC Location '/mnt/iotdata/Gekentekende_Voertuigen/table/tbl_gekentekende_voertuigen_Bd'

# COMMAND ----------

file_bronze_dump = spark.read.json(f'/mnt/iotdata/{file_name}/{datetoday}/*.json')


# COMMAND ----------



# COMMAND ----------

for i in range(len(file_B_DF.columns)-2):
    file_B_DF = file_B_DF.withColumnRenamed(file_B_DF.columns[i], "target_"+file_B_DF.columns[i])

file_B_DF = file_B_DF.drop(file_B_DF[-1]).drop(file_B_DF[-2])
display(file_B_DF)

# COMMAND ----------

joinDF = file_bronze_dump.join(file_B_DF,(file_bronze_dump.kenteken==file_B_DF.target_kenteken), "leftouter").select(file_bronze_dump["*"], file_B_DF["*"])
display(joinDF)

# COMMAND ----------

array_source_columns = array(joinDF.columns[0:int(len(joinDF.columns)/2)])
array_target_columns = array(joinDF.columns[int(len(joinDF.columns)/2):])

# COMMAND ----------

filterDF = joinDF.filter(xxhash64(array_source_columns
)!=xxhash64(array_target_columns))
display(filterDF)

# COMMAND ----------

mergeDF = filterDF.withColumn("MERGEKEY", filterDF.kenteken)
display(mergeDF)

# COMMAND ----------

dummyDF = filterDF.filter(f"{target_pm} is not null").withColumn("MERGEKEY", lit(None))
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

tbl_gekentekende_voertuigen_Bd.alias("target").merge(
source = scdDF.alias("source"),
condition = f"target.{primary_key} = source.MERGEKEY and target.End_Date is null"
).whenMatchedUpdate(set = 
                   {
                       "End_Date" : "current_date"
                   }
).whenNotMatchedInsert(values = dict_delta).execute()
