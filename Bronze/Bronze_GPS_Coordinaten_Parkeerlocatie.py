# Databricks notebook source
from datetime import date
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *

# COMMAND ----------

datetoday = date.today().strftime("%Y-%m-%d")
firstdate = "2023-03-24"
yesterday = "2023-03-30"

# COMMAND ----------

file_name = "GPS-Coordinaten_Parkeerlocatie"
primary_key = "locationreferencetype"
target_pm = "target_"+primary_key
table_name = "tbl_"+file_name+"_Bd"

# COMMAND ----------

file_bronze = spark.read.json(f'/mnt/iotdata/{file_name}/{firstdate}/*.json')
file_bronze = file_bronze.withColumn("Start_Date", lit(firstdate).cast(DateType())).withColumn("End_Date",lit(None).cast(DateType()))
file_bronze.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(f"/mnt/iotdata/{file_name}/table/tbl_GPS_Coordinaten_Parkeerlocatie_Bd")
tbl_file_Bd = DeltaTable.forPath(spark, f"/mnt/iotdata/{file_name}/table/tbl_GPS_Coordinaten_Parkeerlocatie_Bd")
file_B_DF = tbl_file_Bd.toDF()
display(file_B_DF)


# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC Create table GPS_Coordinaten_Parkeerlocatie_delta_b
# MAGIC ()
# MAGIC Location '/mnt/iotdata/GPS-Coordinaten_Parkeerlocatie/table/tbl_GPS_Coordinaten_Parkeerlocatie_Bd'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from GPS_Coordinaten_Parkeerlocatie_delta_b
# MAGIC Where locationreference = "1492"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from geconstateerde_gebreken_delta_b
# MAGIC where meld_datum_door_keuringsinstantie_dt is null

# COMMAND ----------

file_bronze_dump = spark.read.json(f'/mnt/iotdata/{file_name}/{datetoday}/*.json')


# COMMAND ----------

for i in range(len(file_B_DF.columns)-2):
    file_B_DF = file_B_DF.withColumnRenamed(file_B_DF.columns[i], "target_"+file_B_DF.columns[i])

file_B_DF = file_B_DF.drop(file_B_DF[-1]).drop(file_B_DF[-2])
display(file_B_DF)

# COMMAND ----------

joinDF = file_bronze_dump.join(file_B_DF,((file_bronze_dump.locationreference==file_B_DF.target_locationreference) & (file_bronze_dump.startdatelocation ==file_B_DF.target_startdatelocation)), "leftouter").select(file_bronze_dump["*"], file_B_DF["*"])
display(joinDF)

# COMMAND ----------

array_source_columns = array(joinDF.columns[0:int(len(joinDF.columns)/2)])
array_target_columns = array(joinDF.columns[int(len(joinDF.columns)/2):])

# COMMAND ----------

filterDF = joinDF.filter(xxhash64(array_source_columns
)!=xxhash64(array_target_columns))
display(filterDF)

# COMMAND ----------

mergeDF = filterDF.withColumn("MERGEKEY", concat(filterDF.locationreference,filterDF.startdatelocation
))
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

tbl_file_Bd.alias("target").merge(
source = scdDF.alias("source"),
condition = f"concat(target.locationreference, target.startdatelocation) = source.MERGEKEY and target.End_Date is null"
).whenMatchedUpdate(set = 
                   {
                       "End_Date" : "current_date"
                   }
).whenNotMatchedInsert(values = dict_delta).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Geconstateerde_Gebreken_delta_b
# MAGIC ORDER BY Start_Date desc
