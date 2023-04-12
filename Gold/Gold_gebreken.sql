-- Databricks notebook source
Create or Replace Table Gebreken_gold
LOCATION '/mnt/gold/gebreken/table/tbl_gebreken_g'
AS
select 
merk, 
type, 
datum_tenaamstelling, 
catalogusprijs,
maximale_constructiesnelheid, 
YEAR(datum_tenaamstelling) as Year_tenaamstelling,
YEAR(meld_datum_door_keuringsinstantie) as Year_meld_datum_door_keuringsinstantie,
leeftijd_auto,
gebrek_identificatie,
gebrek_omschrijving,
aantal_gebreken_geconstateerd,
meld_datum_door_keuringsinstantie,
kenteken,
roest_meldingen,
roest_meldingen/leeftijd_auto as roest_meldingen_per_jaar,
number_of_damaged_calls,
number_of_damaged_calls/leeftijd_auto as damaged_calls_per_year,
eerste_kleur,
Price_category
FROM Voertuigen_Gebreken_delta_Z

