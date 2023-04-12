-- Databricks notebook source
CREATE or REPLACE TABLE Voertuigen_Brandstof_delta_Z
(
datum_tenaamstelling DATE,
datum_eerste_toelating DATE,
Leeftijd_auto DECIMAL,
category_price STRING,
aantal_dueren INT,
aantal_zitplaatsen INT,
breedte INT,
bruto_bpm INT,
catalogusprijs INT,
eerste_kleur STRING,
handelsbenaming STRING,
inrichting STRING, 
kenteken STRING,
maximale_constructiesnelheid INT,
merk STRING,
taxi_indicator STRING,
tweede_kleur STRING,
type STRING,
uitvoering STRING,
variant STRING,
voertuigsoort STRING,
zuinigheidsclassificatie STRING,
brandstof_omschrijving STRING,
brandstof_verbruik_gecombineerd_wltp DECIMAL,
brandstofverbruik_gecombineerd DECIMAL,
emissiecode_omschrijving STRING,
geluidsniveau_rijdend INT
)
Location '/mnt/zilver/Voertuigen_Brandstof/table/tbl_Voertuigen_Brandstof_Z'

-- COMMAND ----------

INSERT OVERWRITE Voertuigen_Brandstof_delta_Z
SELECT 
cast(concat(substring(GV.datum_tenaamstelling, 1, 4), '-', substring(GV.datum_tenaamstelling,5,2),'-',substring(GV.datum_tenaamstelling,7,2)) as date),
cast(concat(substring(GV.datum_eerste_toelating, 1, 4), '-', substring(GV.datum_eerste_toelating,5,2),'-',substring(GV.datum_eerste_toelating,7,2)) as date),
DATEDIFF(year, cast(concat(substring(GV.datum_eerste_toelating, 1, 4), '-', substring(GV.datum_eerste_toelating,5,2),'-',substring(GV.datum_eerste_toelating,7,2)) as date), GETDATE()) AS Leeftijd_auto,
cast(if(GV.catalogusprijs < 10000, '10K or less', 
if(GV.catalogusprijs < 20000, '10k and 20k', 
if(GV.catalogusprijs < 30000, '20k and 30k', 
if(GV.catalogusprijs < 40000, '30k and 40k', 
if(GV.catalogusprijs < 50000, '40k and 50k', 
if(GV.catalogusprijs < 60000, '50k and 60k', 
if(GV.catalogusprijs < 70000, '60k and 70k', 
if(GV.catalogusprijs < 80000, '70k and 80k', 
if(GV.catalogusprijs < 90000, '80k and 90k', 
if(GV.catalogusprijs < 100000, '90k and 100k', 
if(GV.catalogusprijs > 100000, 'More then 100k', Null))))))))))) as STRING) as category_price,
GV.aantal_deuren,
GV.aantal_zitplaatsen,
GV.breedte,
GV.bruto_bpm,
GV.catalogusprijs,
GV.eerste_kleur,
GV.handelsbenaming,
GV.inrichting,
GV.kenteken,
GV.maximale_constructiesnelheid,
GV.merk,
GV.taxi_indicator,
GV.tweede_kleur,
GV.type,
GV.uitvoering,
GV.variant,
GV.voertuigsoort,
GV.zuinigheidsclassificatie,
GVB.brandstof_omschrijving,
GVB.brandstof_verbruik_gecombineerd_wltp,
GVB.brandstofverbruik_gecombineerd,
GVB.emissiecode_omschrijving,
GVB.geluidsniveau_rijdend
FROM gekentekende_voertuigen_delta_b GV
INNER JOIN gekentekende_voertuigen_brandstof_delta_b GVB
ON GV.kenteken = GVB.kenteken
Where GV.End_Date IS NULL


-- COMMAND ----------

SELECT * FROM Voertuigen_Brandstof_delta_Z
