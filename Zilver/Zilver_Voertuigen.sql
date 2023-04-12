-- Databricks notebook source
CREATE or REPLACE TABLE Voertuigen_delta_Z
(
datum_tenaamstelling DATE,
datum_eerste_toelating DATE,
category_price STRING,
aantal_dueren INT,
aantal_wielen INT,
aantal_zitplaatsen INT,
breedte INT,
bruto_bpm INT,
catalogusprijs INT,
eerste_kleur STRING,
Red INT,
Black INT,
Gray INT,
Blue INT,
Brown INT,
Green INT,
Purple INT,
White INT,
Orange INT,
Yellow INT,
Nvt INT,
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
Year_bought INT
)
Location '/mnt/zilver/Voertuigen/table/tbl_Voertuigen_Z'

-- COMMAND ----------

INSERT OVERWRITE Voertuigen_delta_Z
SELECT 
cast(concat(substring(GV.datum_tenaamstelling, 1, 4), '-', substring(GV.datum_tenaamstelling,5,2),'-',substring(GV.datum_tenaamstelling,7,2)) as date),
cast(concat(substring(GV.datum_eerste_toelating, 1, 4), '-', substring(GV.datum_eerste_toelating,5,2),'-',substring(GV.datum_eerste_toelating,7,2)) as date),
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
GV.aantal_wielen,
GV.aantal_zitplaatsen,
GV.breedte,
GV.bruto_bpm,
GV.catalogusprijs,
GV.eerste_kleur,
Cast(if(GV.eerste_kleur = "ROOD", 1, 0) as INT) as Red,
Cast(if(GV.eerste_kleur = "ZWART", 1, 0)as INT)  as Black,
Cast(if(GV.eerste_kleur = "GRIJS", 1, 0)as INT)  as Gray,
Cast(if(GV.eerste_kleur = "BLAUW", 1, 0)as INT)  as Blue,
Cast(if(GV.eerste_kleur = "BRUIN", 1, 0)as INT)  as Brown,
Cast(if(GV.eerste_kleur = "GROEN", 1, 0)as INT)  as Green,
Cast(if(GV.eerste_kleur = "PAARS", 1, 0)as INT) as Purple,
Cast(if(GV.eerste_kleur = "WIT", 1, 0)as INT) as White,
Cast(if(GV.eerste_kleur = "ORANJE", 1, 0)as INT) as ORANGE,
Cast(if(GV.eerste_kleur = "GEEL", 1, 0)as INT) as Yellow,
Cast(if(GV.eerste_kleur = "N.v.t", 1, 0)as INT) as Nvt,
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
year(cast(concat(substring(GV.datum_tenaamstelling, 1, 4), '-', substring(GV.datum_tenaamstelling,5,2),'-',substring(GV.datum_tenaamstelling,7,2)) as date)) AS Year_bought 
FROM gekentekende_voertuigen_delta_b GV
Where GV.End_Date IS NULL AND
eerste_kleur IS NOT NULL AND
merk IS NOT NULL AND
GV.catalogusprijs IS NOT NULL AND
Gv.catalogusprijs > 1




