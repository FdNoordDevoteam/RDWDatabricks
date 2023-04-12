-- Databricks notebook source
Create or Replace Table Voertuigen_color_type_gold
LOCATION '/mnt/gold/Voertuigen/table/tbl_Voertuigen_Fuel_g'
AS
select merk, 
brandstof_omschrijving , 
brandstofverbruik_gecombineerd, 
geluidsniveau_rijdend,  
Leeftijd_auto, 
catalogusprijs,
category_price
FROM Voertuigen_brandstof_delta_Z
WHERE 
voertuigsoort = "Personenauto" AND
brandstofverbruik_gecombineerd is not NULL AND
geluidsniveau_rijdend is not Null


-- COMMAND ----------


