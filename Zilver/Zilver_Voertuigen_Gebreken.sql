-- Databricks notebook source
CREATE or Replace TABLE Voertuigen_Gebreken_delta_Z
(
Price_category STRING,
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
datum_tenaamstelling Date,
datum_eerste_toelating Date,
aantal_gebreken_geconstateerd INT,
gebrek_identificatie STRING,
meld_datum_door_keuringsinstantie DATE,
gebrek_omschrijving STRING,
roest_meldingen INT,
leeftijd_auto INT,
number_of_damaged_calls INT
)
Location '/mnt/zilver/Voertuigen_Gebreken/table/tbl_Voertuigen_Gebreken_Z'

-- COMMAND ----------

INSERT OVERWRITE Voertuigen_Gebreken_delta_Z
SELECT 
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
cast(concat(substring(GV.datum_tenaamstelling, 1, 4), '-', substring(GV.datum_tenaamstelling,5,2),'-',substring(GV.datum_tenaamstelling,7,2)) as date),
cast(concat(substring(GV.datum_eerste_toelating, 1, 4), '-', substring(GV.datum_eerste_toelating,5,2),'-',substring(GV.datum_eerste_toelating,7,2)) as date),
GB.aantal_gebreken_geconstateerd,
GB.gebrek_identificatie,
cast(concat(substring(GB.meld_datum_door_keuringsinstantie, 1, 4), '-', substring(GB.meld_datum_door_keuringsinstantie,5,2),'-',substring(GB.meld_datum_door_keuringsinstantie,7,2)) as date),
G.gebrek_omschrijving,
cast(if(lower(G.gebrek_omschrijving) like '%roest%', aantal_gebreken_geconstateerd, 0) as INT) as aantal_roest_meldingen,
DATEDIFF(year, cast(concat(substring(GV.datum_eerste_toelating, 1, 4), '-', substring(GV.datum_eerste_toelating,5,2),'-',substring(GV.datum_eerste_toelating,7,2)) as date), GETDATE()) AS Leeftijd_auto,
cast(if(lower(G.gebrek_omschrijving) like '%beschadigd%' or lower(G.gebrek_omschrijving) like '%beschadiging%' or lower(G.gebrek_omschrijving) like '%gebroken%' or lower(G.gebrek_omschrijving) like '%gescheurd%', aantal_gebreken_geconstateerd, 0) as INT) as number_of_damaged_calls
FROM gekentekende_voertuigen_delta_b as GV
LEFT OUTER JOIN geconstateerde_gebreken_delta_b GB
ON GV.kenteken = GB.kenteken
LEFT OUTER JOIN gebreken_table_delta_b G
ON GB.gebrek_identificatie = G.gebrek_identificatie
Where 
GV.End_Date IS NULL AND
GB.End_Date IS NULL 



-- COMMAND ----------

select * from voertuigen_gebreken_delta_z

