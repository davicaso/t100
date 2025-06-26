-- T100 (Market and Segment) (US DoT data)
-- Bureau of Transportation Statistics (TranStats) > Aviation Data Library > Airline On-Time Performance Data Database > Reporting Carrier On-Time Performance (1987-present)
-- https://www.transtats.bts.gov/Tables.asp?QO_VQ=EEE&QO_anzr=Nv4%FDPn44vr4%FDf6n6v56vp5%FD%FLS14z%FDHE%FDg4nssvp%FM-%FDNyy%FDPn44vr45&QO_fu146_anzr=Nv4%FDPn44vr45


-- STEPS:
-- 0. download and unzip individual pre-zipped data files (stored by year and month) from https://transtats.bts.gov/PREZIP/
-- 1. create air_oai_facts.f41_traffic_t100_market_archive table in postgre
-- 2. ingest t100_market csv data using copy command 
-- MARKET --
-- 3. define materialized views to transform the data:
--  3.1. air_oai_facts.airline_traffic_market_integrate_mv 
-- 4. create fact tables by pulling the data from air_oai_facts.airline_traffic_market_integrate_mv with appropriate conditions:
--  4.1. Table air_oai_facts.airline_traffic_segment
--  4.2. Insert values into air_oai_facts.airline_traffic_market
-- SEGMENT --
-- 5. create air_oai_facts.f41_traffic_t100_segment_archive table in postgre
-- 6. ingest t100_segment csv data using copy command 
-- 7. define materialized views to transform the data:
--  7.1. air_oai_facts.f41_traffic_t100_segment_load_mv (only needed for particular older files that require data quality work.)
--  7.2.  air_oai_facts.airline_traffic_segment_integrate_mv
-- 8. create fact tables by pulling the data from air_oai_facts.airline_traffic_market_integrate_mv with appropriate conditions:
--  8.1. Table air_oai_facts.airline_traffic_segment
--  8.2. Insert values into air_oai_facts.airline_traffic_segment
-- 9.  create fact tables extracting data from air_oai_facts.airline_traffic_segment with column descriptions:
--  9.1. air_oai_dims.aircraft_configurations (aircraft_configuration_ref)
-- 10. create fact tables by pulling the data from air_oai_facts.airline_traffic_market with appropriate conditions:
--  10.1. air_oai_dims.airline_service_classes (service_class_code)
-- 11. add keys and indexes
-- 12. vacuum the tables
-- 13. test/validation queries

----------------------------
-- Airline Traffic Market --
----------------------------

-- 1. define a table we'll be copying the data to (alternatively use one of the FDW extension)
-- alter table air_oai_facts.f41_traffic_t100_market_archive rename column unique_airline_oai_code to airline_unique_oai_code;
-- alter table air_oai_facts.f41_traffic_t100_market_archive rename column unique_airline_name to airline_unique_name;
-- alter table air_oai_facts.f41_traffic_t100_market_archive rename column unique_entity_oai_code to entity_unique_oai_code;
DROP TABLE if exists air_oai_facts.f41_traffic_t100_market_archive;
CREATE TABLE air_oai_facts.f41_traffic_t100_market_archive
	( passengers_qty 				float4
	, freight_lbr 					float4
	, mail_lbr 						float4
	, distance_smi 					float4
	, airline_unique_oai_code 		varchar(15) -- unique_airline_oai_code
	, airline_usdot_id 				int4
	, airline_unique_name			varchar(125) -- unique_airline_name
	, entity_unique_oai_code 		varchar(15)  -- unique_entity_oai_code
	, operating_region_code 		varchar(5)
	, airline_oai_code 				varchar(5)
	, airline_name					varchar(125)
	, airline_old_group_nbr 		int4
	, airline_new_group_nbr 		int4
	, depart_airport_oai_id 		int4
	, depart_airport_oai_seq_id 	int4
	, depart_city_market_oai_id 	int4
	, depart_airport_oai_code 		varchar(5)
	, depart_city_name 				varchar(75)
	, depart_subdivision_iso_code 	varchar(5)
	, depart_subdivision_fips_code 	int4
	, depart_subdivision_name 		varchar(75)
	, depart_country_iso_code 		varchar(5)
	, depart_country_name 			varchar(75)
	, depart_world_area_oai_id 		int4
	, arrive_airport_oai_id 		int4
	, arrive_airport_oai_seq_id 	int4
	, arrive_city_market_oai_id 	int4
	, arrive_airport_oai_code 		varchar(5)
	, arrive_city_name 				varchar(75)
	, arrive_subdivision_iso_code 	varchar(5)
	, arrive_subdivision_fips_code 	int4
	, arrive_subdivision_name 		varchar(75)
	, arrive_country_iso_code 		varchar(5)
	, arrive_country_name 			varchar(75)
	, arrive_world_area_oai_id 		int4
	, year_nbr 						int4
	, quarter_nbr 					int4
	, month_nbr 					int4
	, distance_group_id 			int4
	, service_class_code 			varchar(5)
	, data_source_code 				varchar(5)
	);

-- 2. Ingest t100_market csv data using copy command 
copy air_oai_facts.f41_traffic_t100_market_archive
from '/opt/_data/_air/_oai/_t100/T_T100_MARKET_ALL_CARRIER_2022.csv'
-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_t100/_data-market/T100_MARKET_ALL_CARRIER_ALL_2019.csv'
delimiter ',' header csv;


-- select count(*) from air_oai_facts.f41_traffic_t100_market_archive;
-- select * from air_oai_facts.f41_traffic_t100_market_archive limit 100;
-- select * from air_oai_dims.airline_entities;
-- select * from air_oai_dims.airport_history;

-- 3. define materialized views to transform the data
-- 3.1. air_oai_facts.airline_traffic_market_integrate_mv 
drop materialized view if EXISTS air_oai_facts.airline_traffic_market_integrate_mv;
CREATE MATERIALIZED VIEW air_oai_facts.airline_traffic_market_integrate_mv 
AS
SELECT (f.year_nbr::char(4) || lpad(f.month_nbr::varchar(2),2,'0'))::integer as year_month_nbr
     , f.service_class_code
     , f.airline_usdot_id
     , f.airline_oai_code
     , f.entity_unique_oai_code as entity_oai_code
     , ae.source_from_date as airline_effective_date
     , ae.airline_entity_id
     , ae.airline_entity_key
     , f.airline_name
     , f.airline_unique_name
     , f.depart_airport_oai_code
     , h1.effective_from_date as depart_airport_effective_date
     , h1.airport_history_id as depart_airport_history_id
     , h1.airport_history_key as depart_airport_history_key
     , f.arrive_airport_oai_code
     , h2.effective_from_date as arrive_airport_effective_date
     , h2.airport_history_id as arrive_airport_history_id
     , h2.airport_history_key as arrive_airport_history_key
     , f.data_source_code
     , f.passengers_qty
     , f.freight_lbr
     , f.mail_lbr
     , current_user::varchar(32) as created_by
     , current_timestamp::timestamp(0) as created_tmst
from air_oai_facts.f41_traffic_t100_market_archive f
--left outer join calendar.year_month_v c on f.year_nbr = c.year_nbr and f.month_nbr = c.month_of_year_nbr
left outer join air_oai_dims.airline_entities ae
  on f.airline_usdot_id = ae.airline_usdot_id
 and f.airline_oai_code = ae.airline_oai_code
 and f.entity_unique_oai_code = ae.entity_unique_oai_code
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date >= ae.source_from_date
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date
   < case when ae.source_thru_date is null then current_date else ae.source_thru_date end
left outer join air_oai_dims.airport_history h1
  on f.depart_airport_oai_id = h1.airport_oai_id
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date >= h1.effective_from_date
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date 
   < case when h1.effective_thru_date is null then current_date else h1.effective_thru_date end
left outer join air_oai_dims.airport_history h2
  on f.arrive_airport_oai_id = h2.airport_oai_id
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date >= h2.effective_from_date
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date 
   < case when h2.effective_thru_date is null then current_date else h2.effective_thru_date end
order by f.airline_oai_code, f.depart_airport_oai_code, f.arrive_airport_oai_code
--limit 1000
;

-- check if materialized view was created succesfully.
-- select * from air_oai_facts.airline_traffic_market_integrate_mv limit 1000;

-- 4. create fact tables by pulling the data from air_oai_facts.airline_traffic_market_integrate_mv
-- 4.1 create a new table air_oai_facts.airline_traffic_market
drop table if exists air_oai_facts.airline_traffic_market;
CREATE TABLE air_oai_facts.airline_traffic_market 
	( airline_traffic_market_key				char(32)		not null
	, year_month_nbr							integer			not null
	, airline_oai_code 							varchar(3) 		not null
	, airline_effective_date					date			not null
	, airline_entity_id							integer			not null
	, airline_entity_key						char(32)		not null
	, depart_airport_oai_code 					char(3) 		not null
	, depart_airport_effective_date				date			not null
	, depart_airport_history_id					integer 		not null
	, depart_airport_history_key				char(32)		not null
	, arrive_airport_oai_code 					char(3) 		not null
	, arrive_airport_effective_date				date			not null
	, arrive_airport_history_id					integer 		not null
	, arrive_airport_history_key				char(32) 		not null
	, service_class_code 						char(1) 		not null
	, data_source_code							varchar(5)		not null
	, passengers_qty 							integer			not null
	, freight_kgm 								numeric(10,1)	
	, mail_kgm 									numeric(10,1)	not null
	, t100_records_qty 							smallint		not null
	, metadata_key								varchar(32)
	, created_by 								varchar(32)		not null
	, created_tmst 								timestamp(0)	not null
	, updated_by 								varchar(32)
	, updated_tmst 								timestamp(0)
	, constraint airline_traffic_market_pk PRIMARY KEY (airline_traffic_market_key) 
	);


-- 4.2 Insert values into air_oai_facts.airline_traffic_market
-- from the materialized view air_oai_facts.airline_traffic_market_integrate_mv
-- with some ETL process
INSERT INTO air_oai_facts.airline_traffic_market
( airline_traffic_market_key, year_month_nbr, service_class_code
, airline_oai_code, airline_effective_date, airline_entity_id, airline_entity_key
, depart_airport_oai_code, depart_airport_effective_date, depart_airport_history_id, depart_airport_history_key
, arrive_airport_oai_code, arrive_airport_effective_date, arrive_airport_history_id, arrive_airport_history_key
, data_source_code, passengers_qty, freight_kgm, mail_kgm, t100_records_qty
, created_by, created_tmst)
SELECT md5(year_month_nbr::char(6)
    ||'|'||service_class_code
    ||'|'||airline_entity_key
    ||'|'||depart_airport_history_key
    ||'|'||arrive_airport_history_key
    ) as airline_traffic_market_key2
	 , year_month_nbr
	 , service_class_code
	 , max(airline_oai_code) as airline_oai_code
	 , max(airline_effective_date) as airline_effective_date
	 , max(airline_entity_id) as airline_entity_id
	 , airline_entity_key
	 , max(depart_airport_oai_code) as depart_airport_oai_code
	 , max(depart_airport_effective_date) as depart_airport_effective_date
	 , max(depart_airport_history_id) as depart_airport_history_id
	 , depart_airport_history_key
	 , max(arrive_airport_oai_code) as arrive_airport_oai_code
	 , max(arrive_airport_effective_date) as arrive_airport_effective_date
	 , max(arrive_airport_history_id) as arrive_airport_history_id
	 , arrive_airport_history_key
	 , max(data_source_code) as data_source_code
	 , sum(passengers_qty) as passengers_qty
	 , (sum(freight_lbr)*0.45359237)::numeric(10,1) as freight_kgm
	 , (sum(mail_lbr)*0.45359237)::numeric(10,1) as mail_kgm
	 , count(*) as t100_records_qty
     , current_user::varchar(32) as created_by
     , current_timestamp::timestamp(0) as created_tmst
FROM air_oai_facts.airline_traffic_market_integrate_mv
WHERE year_month_nbr is not null 
and service_class_code is not null 
and airline_entity_key is not null 
and depart_airport_history_key is not null 
and arrive_airport_history_key is not null
--and year_month_nbr between 201001 and 202012 -- 200001 and 200912 -- 199601 and 199912 -- 199101 and 199512
--and year_month_nbr::char(6) like '1990%'
GROUP BY year_month_nbr
     , service_class_code
	 , airline_entity_key --, airline_oai_code, airline_effective_date
	 , depart_airport_history_key --, depart_airport_oai_code, depart_airport_effective_date
	 , arrive_airport_history_key -- , arrive_airport_oai_code, arrive_airport_effective_date
	 ;

-- select * from air_oai_facts.airline_traffic_market; -- 25544
-- select count(*) from air_oai_facts.airline_traffic_market; -- 25544
-- select count(*) from air_oai_facts.f41_traffic_t100_market_archive; -- 25948 #check the dataset
-- drop materialized view if exists air_oai_facts.airline_traffic_market_integrate_mv;
-- drop table if exists air_oai_facts.f41_traffic_t100_market_archive;

-----------------------------
-- Airline Traffic Segment --
-----------------------------
-- 5. define a table we'll be copying the data to (alternatively use one of the FDW extension)
DROP TABLE IF EXISTS air_oai_facts.f41_traffic_t100_segment_archive;
CREATE TABLE air_oai_facts.f41_traffic_t100_segment_archive
	( scheduled_departures_qty	 			float4
	, performed_departures_qty	 			float4
	, payload_lbr 							float4
	, available_seat_qty 					float4
	, passengers_qty 						float4
	, freight_lbr 							float4
	, mail_lbr 								float4
	, distance_smi 							float4
	, ramp_to_ramp_min 						float4
	, air_time_min 							float4
	, airline_unique_oai_code 				varchar(10)
	, airline_usdot_id 						int4
	, airline_unique_name 					varchar(125)
	, entity_unique_oai_code 				varchar(15)
	, operating_region_code 				varchar(25)
	, airline_oai_code 						varchar(5)
	, airline_name 							varchar(125)
	, airline_old_group_nbr					int4
	, airline_new_group_nbr 				int4
	, depart_airport_oai_id 				int4
	, depart_airport_oai_seq_id 			int4
	, depart_market_city_oai_id 			int4
	, depart_airport_oai_code 				varchar(3)
	, depart_city_name 						varchar(75)
	, depart_state_cd 						varchar(5)
	, depart_state_fips_cd 					varchar(5)
	, depart_state_nm 						varchar(75)
	, depart_country_iso_code 				varchar(10)
	, depart_country_name 					varchar(75)
	, depart_world_area_oai_id 				int4
	, arrive_airport_oai_id 				int4
	, arrive_airport_oai_seq_id 			int4
	, arrive_market_city_oai_id 			int4
	, arrive_airport_oai_code 				varchar(5)
	, arrive_city_name 						varchar(75)
	, arrive_subdivision_iso_code 			varchar(5)
	, arrive_subdivision_fips_code 			varchar(5)
	, arrive_subdivision_name 				varchar(75)
	, arrive_country_iso_code 				varchar(10)
	, arrive_country_name 					varchar(75)
	, arrive_world_area_oai_id 				int4
	, aircraft_group_oai_nbr				int4 -- 
	, aircraft_type_oai_nbr 				int4 -- 
	, aircraft_configuration_id 			int4
	, year_nbr 								int4
	, quarter_nbr 							int4
	, month_nbr 							int4
	, distance_group_id 					int4
	, service_class_code 					char(1)
	, data_source_code 						varchar(5)
	--, filler_txt 							varchar(10)
	);

-- 6. Ingest t100_segment csv data using copy command
copy air_oai_facts.f41_traffic_t100_segment_archive
from '/opt/_data/_air/_oai/_t100/T_T100_SEGMENT_ALL_CARRIER_2022.csv'
delimiter ',' header csv;

-- check if the table was created succesfully.
--select * from air_oai_facts.f41_traffic_t100_segment_archive;

-- 7. define materialized views:
-- 7.1. define materialized view for initial data quality work.
-- this is only needed for particular older files that require data quality work.
drop materialized view air_oai_facts.f41_traffic_t100_segment_load_mv;
create materialized view air_oai_facts.f41_traffic_t100_segment_load_mv as
SELECT scheduled_departures_qty
	, performed_departures_qty
	, payload_lbr
	, available_seat_qty
	, passengers_qty
	, freight_lbr
	, mail_lbr
	, distance_smi
	, ramp_to_ramp_min
	, air_time_min
	, airline_unique_oai_code
	, airline_usdot_id
	--, case when airline_oai_code = '5G' and airline_usdot_id is null then 21181
	       --when airline_oai_code = '0OQ' and airline_usdot_id is null then 21287
	       --when airline_oai_code = 'AQ' and airline_usdot_id is null then 19678
	       --when airline_oai_code = 'KH' and airline_usdot_id = 19678 then 21634
	       --airline_oai_code = 'K8' and airline_usdot_id is null then 20310
	       --airline_oai_code = 'XP' and airline_usdot_id is null then 20207
	       --when airline_oai_code = '2HQ' is not null and airline_usdot_id is null then 21712
	--       else airline_usdot_id end::integer as airline_usdot_id
	, airline_unique_name
	, entity_unique_oai_code
	--, case when airline_oai_code = '5G' and airline_usdot_id is null then '71032'
	       --when airline_oai_code = '0OQ' and airline_usdot_id is null then '71056'
	       --when airline_oai_code = 'AQ' and (airline_usdot_id is null or airline_usdot_id = 19678)
	       -- and (depart_country_iso_code in ('US','CA') and arrive_country_iso_code in ('US','CA')) then '05045'
	       --when airline_oai_code = 'AQ' and (airline_usdot_id is null or airline_usdot_id = 19678)
	       -- and (depart_country_iso_code != 'US' or arrive_country_iso_code != 'US') then '15045'
	       --when airline_oai_code = 'K8' and airline_usdot_id is null
	       -- and (depart_country_iso_code != 'US' or arrive_country_iso_code != 'US') then '16076'
	       --when airline_oai_code = 'K8' and airline_usdot_id is null
	       -- and (depart_country_iso_code = 'US' and arrive_country_iso_code = 'US') then '06076'
	       --when airline_oai_code = 'XP' and airline_usdot_id is null
	       -- and (depart_country_iso_code != 'US' or arrive_country_iso_code != 'US') then '16144'
	       --when airline_oai_code = 'XP' and airline_usdot_id is null
	       -- and (depart_country_iso_code = 'US' and arrive_country_iso_code = 'US') then '06144'
	       --when airline_oai_code = '2HQ' is not null and airline_usdot_id is null
	       -- and depart_country_iso_code = 'US' and arrive_country_iso_code = 'US' then '01200'
	       --when airline_oai_code = '2HQ' is not null and airline_usdot_id is null
	       -- and (depart_country_iso_code != 'US' or arrive_country_iso_code != 'US') then '11047'
	--       else entity_unique_oai_code end::varchar(15) as entity_unique_oai_code
	, operating_region_code
	, airline_oai_code
	--, case when airline_oai_code = '39Q' and airline_usdot_id = 21894 then 'AN'
	--       when airline_oai_code = '3GQ' and airline_usdot_id = 21869 then '36Q'
	--       when airline_oai_code = 'A0' and airline_usdot_id = 20234 and unique_entity_oai_code = '9486F' then '8R'
	--       else airline_oai_code end::varchar(5) as airline_oai_code
	, airline_name
	, airline_old_group_nbr
	, airline_new_group_nbr
	, depart_airport_oai_id
	, depart_airport_oai_seq_id
	, depart_market_city_oai_id
	, depart_airport_oai_code
	, depart_city_name
	, depart_state_cd
	, depart_state_fips_cd
	, depart_state_nm
	, depart_country_iso_code
	, depart_country_name
	, depart_world_area_oai_id
	, arrive_airport_oai_id
	, arrive_airport_oai_seq_id
	, arrive_market_city_oai_id
	, arrive_airport_oai_code
	, arrive_city_name
	, arrive_subdivision_iso_code
	, arrive_subdivision_fips_code
	, arrive_subdivision_name
	, arrive_country_iso_code
	, arrive_country_name
	, arrive_world_area_oai_id
	, aircraft_group_oai_nbr
	, aircraft_type_oai_nbr
	, aircraft_configuration_id
	, year_nbr
	, quarter_nbr
	, month_nbr
	, distance_group_id
	, service_class_code
	, data_source_code
	--, filler_txt
FROM air_oai_facts.f41_traffic_t100_segment_archive;
--limit 1000;

-- check if materialized view was created succesfully.
--elect * from air_oai_facts.f41_traffic_t100_segment_load_mv;

-- 7.2. define materialized view for initial data quality work.
drop materialized view air_oai_facts.airline_traffic_segment_integrate_mv;
CREATE materialized view air_oai_facts.airline_traffic_segment_integrate_mv as
SELECT (f.year_nbr::char(4) || lpad(f.month_nbr::varchar(2),2,'0'))::integer as year_month_nbr
     , f.service_class_code
     , f.airline_usdot_id
     , f.airline_oai_code
     , f.entity_unique_oai_code as entity_oai_code
     , ae.source_from_date as airline_effective_date
     , ae.airline_entity_id
     , ae.airline_entity_key
     , f.airline_name
     , f.airline_unique_name
     , f.depart_airport_oai_code
     , h1.effective_from_date as depart_airport_effective_date
     , h1.airport_history_id as depart_airport_history_id
     , h1.airport_history_key as depart_airport_history_key
     , f.arrive_airport_oai_code
     , h2.effective_from_date as arrive_airport_effective_date
     , h2.airport_history_id as arrive_airport_history_id
     , h2.airport_history_key as arrive_airport_history_key
     , f.aircraft_type_oai_nbr
	 , case when f.aircraft_configuration_id = 0 then 'N/A'
	        when f.aircraft_configuration_id = 1 then 'PAX'
	        when f.aircraft_configuration_id = 2 then 'FRT'
	        when f.aircraft_configuration_id = 3 then 'CMB'
	        when f.aircraft_configuration_id = 4 then 'SEA'
	        when f.aircraft_configuration_id = 9 then 'EXP'
	        else 'UNK' end::char(3) as aircraft_configuration_ref
     , f.data_source_code
     , f.passengers_qty
     , f.freight_lbr
     , f.mail_lbr
     , f.available_seat_qty
     , f.scheduled_departures_qty
     , f.performed_departures_qty
     , f.ramp_to_ramp_min
     , f.air_time_min
     , current_user::varchar(32) as created_by
     , current_timestamp::timestamp(0) as created_tmst
from air_oai_facts.f41_traffic_t100_segment_archive f
-- left outer join calendar.year_month_v c on f.year_nbr = c.year_nbr and f.month_nbr = c.month_of_year_nbr
left outer join air_oai_dims.airline_entities ae
  on f.airline_usdot_id = ae.airline_usdot_id
 and f.airline_oai_code = ae.airline_oai_code
 and f.entity_unique_oai_code = ae.entity_unique_oai_code
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date >= ae.source_from_date
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date 
   < case when ae.source_thru_date is null then current_date else ae.source_thru_date end
left outer join air_oai_dims.airport_history h1
  on f.depart_airport_oai_id = h1.airport_oai_id
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date >= h1.effective_from_date
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date 
   < case when h1.effective_thru_date is null then current_date else h1.effective_thru_date end
left outer join air_oai_dims.airport_history h2
  on f.arrive_airport_oai_id = h2.airport_oai_id
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date >= h2.effective_from_date
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date 
   < case when h2.effective_thru_date is null then current_date else h2.effective_thru_date end
order by f.airline_oai_code, f.depart_airport_oai_code, f.arrive_airport_oai_code;

-- check if materialized view was created succesfully.
--select * from air_oai_facts.airline_traffic_segment_integrate_mv;

-- 8. create a new table to copy the values and think about the ETL process
-- 8.1. create air_oai_facts.airline_traffic_segment 
-- drop table if exists air_oai_facts.airline_traffic_segment;
CREATE TABLE air_oai_facts.airline_traffic_segment 
	( airline_traffic_segment_key				char(32)		not null
	, year_month_nbr							integer			not null
	, service_class_code 						char(1) 		not null	
	, airline_oai_code 							varchar(3) 		not null
	, airline_effective_date					date			not null
	, airline_entity_id							integer			not null
	, airline_entity_key						char(32)		not null
	, depart_airport_oai_code 					char(3) 		not null
	, depart_airport_effective_date				date			not null
	, depart_airport_history_id					integer 		not null
	, depart_airport_history_key				char(32)		not null
	, arrive_airport_oai_code 					char(3) 		not null
	, arrive_airport_effective_date				date			not null
	, arrive_airport_history_id					integer 		not null
	, arrive_airport_history_key				char(32) 		not null
	, aircraft_type_oai_nbr						integer			not null
	, aircraft_configuration_ref				char(3)			not null
	, data_source_code							varchar(5)		not null
	, scheduled_departures_qty					integer			not null
	, performed_departures_qty					integer			not null
	, available_seat_qty						integer			not null
	, passengers_qty 							integer			not null
	, freight_kgm 								numeric(10,1)	not null -- originally lbr, convert to kgm
	, mail_kgm 									numeric(10,1)	not null -- originally lbr, convert to kgm
	, ramp_to_ramp_min							integer			not null
	, air_time_min								integer			not null
	, t100_records_qty							smallint		not null
	, metadata_key								varchar(32)
	, created_by 								varchar(32)		not null
	, created_tmst 								timestamp(0)	not null
	, updated_by 								varchar(32)
	, updated_tmst 								timestamp(0)
	, constraint airline_traffic_segment_pk PRIMARY KEY (airline_traffic_segment_key) 
	);


-- 8.2. Insert values into air_oai_facts.airline_traffic_segment
-- from the materialized view air_oai_facts.airline_traffic_segment_integrate_mv
-- with some ETL process
INSERT INTO air_oai_facts.airline_traffic_segment
( airline_traffic_segment_key, year_month_nbr, service_class_code
, airline_oai_code, airline_effective_date, airline_entity_id, airline_entity_key
, depart_airport_oai_code, depart_airport_effective_date, depart_airport_history_id, depart_airport_history_key
, arrive_airport_oai_code, arrive_airport_effective_date, arrive_airport_history_id, arrive_airport_history_key
, aircraft_type_oai_nbr, aircraft_configuration_ref, data_source_code, scheduled_departures_qty, performed_departures_qty
, available_seat_qty, passengers_qty, freight_kgm, mail_kgm, ramp_to_ramp_min, air_time_min, t100_records_qty
, created_by, created_tmst)
SELECT md5(year_month_nbr::char(6)
    ||'|'||service_class_code
    ||'|'||airline_entity_key
    ||'|'||depart_airport_history_key
    ||'|'||arrive_airport_history_key
    ||'|'||lpad(aircraft_type_oai_nbr::varchar(3),3,'0')
    ||'|'||aircraft_configuration_ref::char(3)
    ) as airline_traffic_segment_key
	 , year_month_nbr
	 , service_class_code
	 , max(airline_oai_code) as airline_oai_code
	 , max(airline_effective_date) as airline_effective_date
	 , max(airline_entity_id) as airline_entity_id
	 , airline_entity_key
	 , max(depart_airport_oai_code) as depart_airport_oai_code
	 , max(depart_airport_effective_date) as depart_airport_effective_date
	 , max(depart_airport_history_id) as depart_airport_history_id
	 , depart_airport_history_key
	 , max(arrive_airport_oai_code) as arrive_airport_oai_code
	 , max(arrive_airport_effective_date) as arrive_airport_effective_date
	 , max(arrive_airport_history_id) as arrive_airport_history_id
	 , arrive_airport_history_key
	 , aircraft_type_oai_nbr
	 , aircraft_configuration_ref
	 , max(data_source_code) as data_source_code
	 , sum(scheduled_departures_qty) as scheduled_departures_qty
	 , sum(performed_departures_qty) as performed_departures_qty
	 , sum(available_seat_qty) as available_seat_qty
	 , sum(passengers_qty) as passengers_qty
	 , (sum(freight_lbr)*0.45359237)::numeric(10,1) as freight_kgm
	 , (sum(mail_lbr)*0.45359237)::numeric(10,1) as mail_kgm
	 , sum(ramp_to_ramp_min) as ramp_to_ramp_min
	 , sum(air_time_min) as air_time_min
	 , count(*) as t100_records_qty
	 , current_user::varchar(32) as created_by
     , current_timestamp::timestamp(0) as created_tmst
FROM air_oai_facts.airline_traffic_segment_integrate_mv
WHERE year_month_nbr is not null and service_class_code is not null and airline_entity_key is not null 
and depart_airport_history_key is not null and arrive_airport_history_key is not null
and aircraft_type_oai_nbr is not null and aircraft_configuration_ref is not null
--and year_month_nbr between 199101 and 199512
--and year_month_nbr::char(6) like '1990%'
GROUP BY year_month_nbr, service_class_code
	 , airline_entity_key --, airline_oai_code, airline_effective_date
	 , depart_airport_history_key --, depart_airport_oai_code, depart_airport_effective_date
	 , arrive_airport_history_key -- , arrive_airport_oai_code, arrive_airport_effective_date
     , aircraft_type_oai_nbr, aircraft_configuration_ref; 

-- check if materialized view was created succesfully.
select * from air_oai_facts.airline_traffic_segment limit 1000;

-- check tables and views rows.
-- select count(*) from air_oai_facts.airline_traffic_segment; -- 44289
-- select count(*) from air_oai_facts.f41_traffic_t100_segment_archive; -- 44941
-- drop materialized view if exists air_oai_facts.airline_traffic_segment_integrate_mv;
-- drop materialized view air_oai_facts.f41_traffic_t100_segment_load_mv;
-- drop table if exists air_oai_facts.f41_traffic_t100_segment_archive;

-- 9. create fact tables by pulling the data from air_oai_facts.airline_flight_performance_integrated_mv with appropriate conditions:
-- air_oai_dims.aircraft_configurations
drop table if exists air_oai_dims.aircraft_configurations;
create table air_oai_dims.aircraft_configurations as
select f.aircraft_configuration_ref
     , max(case when f.aircraft_configuration_ref = 'CMB' then 'Combination Freight and Passenger, Main Deck'
            when f.aircraft_configuration_ref = 'FRT' then 'Freight Only, Main Deck'
            when f.aircraft_configuration_ref = 'PAX' then 'Passenger Only, Main Deck'
            when f.aircraft_configuration_ref = 'SEA' then 'Seaplane'
            else null end::varchar(255)) as aircraft_configuration_descr
     , current_user::varchar(32) as created_by
     , current_timestamp::timestamp(0) as created_ts
     , null::char(32) as updated_by
     , null::timestamp(0) as updated_tmst
from air_oai_facts.airline_traffic_segment f
group by 1 order by 1;


alter table air_oai_dims.aircraft_configurations 
add constraint aircraft_configurations_pk primary key (aircraft_configuration_ref);

-- 10. create fact tables by pulling the data from air_oai_facts.airline_traffic_market with appropriate conditions:
-- air_oai_dims.airline_service_classes
drop table if exists air_oai_dims.airline_service_classes;
create table air_oai_dims.airline_service_classes as
select f.service_class_code
     , max(case when f.service_class_code in ('F','G') then 1 else 0 end::smallint) as scheduled_ind
     , max(case when f.service_class_code in ('L','P') then 1 else 0 end::smallint) as chartered_ind
     , max(case when f.service_class_code = 'F' then 'Scheduled Passenger / Cargo Service'
            when f.service_class_code = 'G' then 'Scheduled CAll Cargo Service'
            when f.service_class_code = 'L' then 'Non-Scheduled Civilian Passenger / Cargo Service'
            when f.service_class_code = 'P' then 'Non-Scheduled Civilian All Cargo Service'
            else null end::varchar(255)) as service_class_descr
     , current_user::varchar(32) as created_by
     , current_timestamp::timestamp(0) as created_ts
     , null::char(32) as updated_by
     , null::timestamp(0) as updated_tmst
from air_oai_facts.airline_traffic_market f
group by 1 order by 1;

alter table air_oai_dims.airline_service_classes
add constraint airline_service_classes_pk primary key (service_class_code);

-- 11. add keys and indexes
-- air_oai_facts.airline_traffic_market
alter table air_oai_facts.airline_traffic_market add constraint airline_traffic_market_service_fk 
foreign key (service_class_code) references air_oai_dims.airline_service_classes (service_class_code);

-- IDS:
alter table air_oai_facts.airline_traffic_market add constraint airline_traffic_market_airline_id_fk
foreign key (airline_entity_id) references air_oai_dims.airline_entities (airline_entity_id);

alter table air_oai_facts.airline_traffic_market add constraint airline_traffic_market_depart_airport_id_fk
foreign key (depart_airport_history_id) references air_oai_dims.airport_history (airport_history_id);

alter table air_oai_facts.airline_traffic_market add constraint airline_traffic_market_arrive_airport_id_fk 
foreign key (arrive_airport_history_id) references air_oai_dims.airport_history (airport_history_id);

-- KEYS:
alter table air_oai_facts.airline_traffic_market add constraint airline_traffic_market_airline_key_fk 
foreign key (airline_entity_key) references air_oai_dims.airline_entities (airline_entity_key);

alter table air_oai_facts.airline_traffic_market add constraint airline_traffic_market_depart_airport_key_fk 
foreign key (depart_airport_history_key) references air_oai_dims.airport_history (airport_history_key);

alter table air_oai_facts.airline_traffic_market add constraint airline_traffic_market_arrive_airport_key_fk
foreign key (arrive_airport_history_key) references air_oai_dims.airport_history (airport_history_key);


-- air_oai_facts.airline_traffic_segment
alter table air_oai_facts.airline_traffic_segment add constraint airline_traffic_segment_service_fk 
foreign key (service_class_code) references air_oai_dims.airline_service_classes (service_class_code);

alter table air_oai_facts.airline_traffic_segment add constraint airline_traffic_aircraft_configuration_fk 
foreign key (aircraft_configuration_ref) references air_oai_dims.aircraft_configurations (aircraft_configuration_ref);

alter table air_oai_facts.airline_traffic_segment add constraint airline_traffic_aircraft_type_fk 
foreign key (aircraft_type_oai_nbr) references air_oai_dims.aircraft_types (aircraft_type_oai_nbr);

-- IDS:
alter table air_oai_facts.airline_traffic_segment add constraint airline_traffic_segment_airline_id_fk 
foreign key (airline_entity_id) references air_oai_dims.airline_entities (airline_entity_id);

alter table air_oai_facts.airline_traffic_segment add constraint airline_traffic_segment_depart_airport_id_fk 
foreign key (depart_airport_history_id) references air_oai_dims.airport_history (airport_history_id);

alter table air_oai_facts.airline_traffic_segment add constraint airline_traffic_segment_arrive_airport_id_fk 
foreign key (arrive_airport_history_id) references air_oai_dims.airport_history (airport_history_id);

-- KEYS:
alter table air_oai_facts.airline_traffic_segment add constraint airline_traffic_segment_airline_key_fk 
foreign key (airline_entity_key) references air_oai_dims.airline_entities (airline_entity_key);

alter table air_oai_facts.airline_traffic_segment add constraint airline_traffic_segment_depart_airport_key_fk 
foreign key (depart_airport_history_key) references air_oai_dims.airport_history (airport_history_key);

alter table air_oai_facts.airline_traffic_segment add constraint airline_traffic_segment_arrive_airport_key_fk 
foreign key (arrive_airport_history_key) references air_oai_dims.airport_history (airport_history_key);

-- 12. vacuum the tables
vacuum analyze air_oai_dims.aircraft_configurations;
vacuum analyze air_oai_dims.airline_service_classes;

-- 13. validation