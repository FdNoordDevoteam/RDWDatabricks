-- Databricks notebook source
Create or Replace Table Parkeren_gold
LOCATION '/mnt/gold/Parkeren/table/tbl_Parkeren_g'
AS select 
place,
province,
parkingaddressreferencetype,
latitude,
longitude
FROM parkeren_delta_z

