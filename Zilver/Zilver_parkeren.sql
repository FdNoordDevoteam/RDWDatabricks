-- Databricks notebook source
-- CREATE OR REPLACE Table parkeren_delta_z 
-- (
--   parkingaddressreference int,
--   place STRING,
--   province STRING,
--   streetname STRING,
--   housenumber int,
--   parkingaddressreferencetype STRING,
--   latitude double,
--   longitude double,
--   startdatelocation BIGINT
-- )
-- LOCATION '/mnt/zilver/Parkeren/table/tbl_Parkderen_Z'



-- COMMAND ----------

Insert OVERWRITE parkeren_delta_z
Select adres.parkingaddressreference, 
adres.place, 
adres.province, 
adres.streetname, 
adres.housenumber, 
adres.parkingaddressreferencetype, 
gps.latitude, 
gps.longitude,
gps.startdatelocation 
from parkeeradres_delta_b adres
JOIN gps_coordinaten_parkeerlocatie_delta_b gps 
on gps.locationreference = adres.parkingaddressreference and 
gps.locationreferencetype = adres.parkingaddressreferencetype
WHERe
gps.End_Date is Null AND
gps.latitude IS NOT NULL AND 
gps.latitude != '0' AND
gps.longitude is not null AND 
gps.longitude != '0' AND
gps.enddatelocation is null AND
adres.parkingaddressreferencetype = 'I-O' 

-- COMMAND ----------

Delete from parkeren_delta_z 
where (parkingaddressreference = 1493 and  startdatelocation = 20171207120655) or 
(parkingaddressreference = 2855 and startdatelocation = 20221205142217) or
(parkingaddressreference = 1492 and startdatelocation = 20171207120300 ) or
(parkingaddressreference = 2853 and startdatelocation = 20221205135512 ) or
(parkingaddressreference = 2854 and startdatelocation = 20221205141236 ) or
(parkingaddressreference = 2856 and startdatelocation = 20221205144514 ) 
