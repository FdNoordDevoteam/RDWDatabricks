-- Databricks notebook source
Create or Replace Table Gebreken_groupby_kenteken_gold
LOCATION '/mnt/gold/gebreken/table/tbl_gebreken_groupby_kenteken_g'
AS select 
kenteken,
merk,
max(price_category) as price_category,
avg(catalogusprijs) as price,
avg(YEAR(datum_tenaamstelling)) as Year_tenaamstelling,
avg(YEAR(meld_datum_door_keuringsinstantie)) as Year_meld_datum_door_keuringsinstantie,
avg(DATEDIFF(year , datum_eerste_toelating, GETDATE())) AS Leeftijd_auto,
sum(if(aantal_gebreken_geconstateerd is null, 0 , aantal_gebreken_geconstateerd )) as totaal_gebreken,
cast(if(sum(aantal_gebreken_geconstateerd)is NOT NULL, 'yes', 'no') as STRING) as gebrek_yes_no,
avg(cast(if(if(aantal_gebreken_geconstateerd is null, 0 , aantal_gebreken_geconstateerd ) = 0, 0 , if(aantal_gebreken_geconstateerd is null, 0 , aantal_gebreken_geconstateerd )/leeftijd_auto) as double)) as gebreken_per_leeftijd_auto
FROM Voertuigen_Gebreken_delta_Z
Where catalogusprijs is not null
GROUP BY kenteken, merk


-- COMMAND ----------

SELECT * from Gebreken_groupby_kenteken_gold
