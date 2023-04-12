-- Databricks notebook source
Create or Replace Table Taxi_gold
LOCATION '/mnt/gold/gebreken/table/tbl_Taxi_g'
AS select 
taxi_indicator,
kenteken,
merk,
uitvoering,
type,
eerste_kleur,
voertuigsoort,
aantal_gebreken_geconstateerd,
gebrek_omschrijving,
datum_eerste_toelating,
DATEDIFF(year , datum_eerste_toelating, GETDATE()) AS Leeftijd_auto
FROM Voertuigen_Gebreken_delta_Z


