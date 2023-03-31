# Databricks notebook source
from datetime import date
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
import json

# COMMAND ----------

datetoday = date.today().strftime("%Y-%m-%d")
firstdate = "2023-03-24"


# COMMAND ----------

gebreken_bronze_dump = spark.read.json(f'/mnt/iotdata/Gebreken/{datetoday}/*.json')
# Geconstateerde_Gebreken_bronze_dump = spark.read.json(f'/mnt/iotdata/Geconstateerde_Gebreken/{firstdate}/*.json')
# Gekentekende_Voertuigen_bronze_dump = spark.read.json(f'/mnt/iotdata/Gekentekende_Voertuigen/{firstdate}/*.json')
# Gekentekende_Voertuigen_Brandstof_bronze_dump = spark.read.json(f'/mnt/iotdata/Gekentekende_Voertuigen_Brandstof/{firstdate}/*.json')
# GPS_Coordinaten_Parkeerlocatie_bronze_dump = spark.read.json(f'/mnt/iotdata/GPS-Coordinaten_Parkeerlocatie/{firstdate}/*.json')
# Parkeeradres_bronze_dump = spark.read.json(f'/mnt/iotdata/Parkeeradres/{firstdate}/*.json')


# COMMAND ----------

gebreken_bronze = spark.read.json(f'/mnt/iotdata/Gebreken/{firstdate}/*.json')

# COMMAND ----------

display(Geconstateerde_Gebreken_bronze_dump.count())

# COMMAND ----------

gebreken_bronze_dump= gebreken_bronze_dump.withColumn("Start_Date", lit(datetoday).cast(Datetype())).withColumn("End_Date",lit(None).cast(DateType()))
# Geconstateerde_Gebreken_bronze_dump = Geconstateerde_Gebreken_bronze_dump.withColumn("Start_Date", lit(firstdate).cast(DateType())).withColumn("End_Date", lit(None).cast(DateType()))
# Gekentekende_Voertuigen_bronze_dump = Gekentekende_Voertuigen_bronze_dump.withColumn("Start_Date", lit(firstdate).cast(DateType())).withColumn("End_Date", lit(None).cast(DateType()))
# Gekentekende_Voertuigen_Brandstof_bronze_dump = Gekentekende_Voertuigen_Brandstof_bronze_dump.withColumn("Start_Date", lit(firstdate).cast(DateType())).withColumn("End_Date", lit(None).cast(DateType()))
# GPS_Coordinaten_Parkeerlocatie_bronze_dump = GPS_Coordinaten_Parkeerlocatie_bronze_dump.withColumn("Start_Date", lit(firstdate).cast(DateType())).withColumn("End_Date", lit(None).cast(DateType()))
# Parkeeradres_bronze_dump = Parkeeradres_bronze_dump.withColumn("Start_Date", lit(firstdate).cast(DateType())).withColumn("End_Date", lit(None).cast(DateType()))

# COMMAND ----------

gebreken_bronze= gebreken_bronze.withColumn("Start_Date", lit(firstdate).cast(StringType())).withColumn("End_Date",lit(None).cast(StringType()))


# COMMAND ----------

# gebreken_bronze_dump.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("tbl_gebreken_Bd")
# Geconstateerde_Gebreken_bronze_dump.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("tbl_Geconstateerde_Gebreken_Bd")
# Gekentekende_Voertuigen_bronze_dump.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("tbl_Gekentekende_Voertuigen_Bd")
# Gekentekende_Voertuigen_Brandstof_bronze_dump.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("tbl_Gekentekende_Voertuigen_Brandstof_Bd")
# GPS_Coordinaten_Parkeerlocatie_bronze_dump.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("tbl_GPS_Coordinaten_Parkeerlocatie_Bd")
# Parkeeradres_bronze_dump.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("tbl_Parkeeradres_Bd")


# COMMAND ----------

display(gebreken_bronze)

# COMMAND ----------

schema = StructType([StructField("einddatum_gebrek_dt", StringType(), True),\
                     StructField("gebrek_identificatie", StringType(), True),\
                     StructField("gebrek_omschrijving", StringType(), True),\
                     StructField("ingangsdatum_gebrek_dt", StringType(), True),\
                     StructField("Start_Date", StringType(), True),\
                     StructField("End_Date", StringType(), True),\
                    ])

# COMMAND ----------

Data = [(None, "DAV", "Waarschuwingsinrichting eCall-boordsysteem geeft defect aan", "2018-05-20T00:00:00.000", datetoday , None  ),
       (None, "MAR", "Test Test Test Test","2018-05-20T00:00:00.000", datetoday, None ),
       (None, "FOK", "Dit is de tweede test","2018-05-20T00:00:00.000", datetoday, None )]
data = spark.createDataFrame(data = Data, schema = schema)
gebreken_bronze_dump = gebreken_bronze_dump.union(data)

# COMMAND ----------

for i in range(len(gebreken_bronze.columns)):
    gebreken_bronze = gebreken_bronze.withColumnRenamed(gebreken_bronze.columns[i],"target_"+gebreken_bronze.columns[i])
    


# COMMAND ----------

gebreken_bronze_dump = gebreken_bronze_dump.withColumn("gebrek_omschrijving", when(col("gebrek_identificatie") == "010", "aanpassing").otherwise(col("gebrek_omschrijving")))

# COMMAND ----------

display(gebreken_bronze_dump)

# COMMAND ----------

joindf = gebreken_bronze_dump.join(gebreken_bronze, (gebreken_bronze_dump.gebrek_identificatie == gebreken_bronze.target_gebrek_identificatie), "leftouter")\
.select(gebreken_bronze_dump["*"], gebreken_bronze["*"])
    

# COMMAND ----------

joindf.einddatum_gebrek_dt

# COMMAND ----------

display(joindf)

# COMMAND ----------

filterDF = joindf.filter(xxhash64(joindf[joindf.columns[0]], joindf[joindf.columns[1]], joindf[joindf.columns[2]], joindf[joindf.columns[3]])!=(xxhash64(joindf[joindf.columns[6]], joindf[joindf.columns[7]], joindf[joindf.columns[8]], joindf[joindf.columns[9]])))

# COMMAND ----------

display(filterDF)

# COMMAND ----------

mergeDF = filterDF.withColumn("MERGEKEY", filterDF[filterDF.columns[1]])

# COMMAND ----------

dummyDF = filterDF.filter("target_gebrek_identificatie is not null").withColumn("MERGEKEY", lit(None))

# COMMAND ----------

display(dummyDF)

# COMMAND ----------

scdDF = mergeDF.union(dummyDF)


# COMMAND ----------



# COMMAND ----------

gebreken_bronze.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save("/tbl_gebreken_Bd")
tbl_gebreken_Bd = DeltaTable.forPath(spark, "/tbl_gebreken_Bd")

# COMMAND ----------

gebreken_bronze.write.format("delta").save("/Gebreken_B_DT")


# COMMAND ----------

Gebreken_B_DT = DeltaTable.forPath(spark, "/Gebreken_B_DT")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Gebreken_B_DT

# COMMAND ----------

display(scdDF)

# COMMAND ----------



# COMMAND ----------

Gebreken_B_DT.alias("target").merge(
    source = scdDF.alias("source"),
    condition = "target.gebrek_identificatie = source.MERGEKEY"
).whenMatchedUpdate(set=
                   {"Target.End_Date": "current_date"}
                   ).whenNotMatchedInsert(values =
                                         {
                                             "target.ingangsdatum_gebrek_dt" : "source.ingangsdatum_gebrek_dt",
                                             "target.gebrek_identificatie" : "source.gebrek_identificatie",
                                             "target.gebrek_omschrijving" : "source.gebrek_omschrijving",
                                             "target.ingangsdatum_gebrek_dt" : "source.ingangsdatum_gebrek_dt",
                                             "target.Start_Date" : "current_date",
                                             "target.End_Date" : "Null"
                                         }
                                         ).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table tbl_gps_coordinaten_parkeerlocatie_bd 

# COMMAND ----------


