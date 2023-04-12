-- Databricks notebook source
Create or Replace Table Voertuigen_Gold
LOCATION '/mnt/gold/Voertuigen/table/tbl_Voertuigen_g'
AS select 
catalogusprijs,
category_price,
eerste_kleur,
maximale_constructiesnelheid,
merk,
voertuigsoort,
Black,
Gray,
Blue,
Brown,
Green,
Red,
Purple,
White,
Orange,
Yellow,
Nvt,
Year_bought

FROM Voertuigen_delta_Z
