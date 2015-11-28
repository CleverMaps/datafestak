BEGIN;
\copy transactions_03 FROM '/var/local/datafest_data/DATAFEST_TRANSACTIONS_P20150331.csv' WITH CSV HEADER DELIMITER '|' QUOTE '^';
\e 'transactions_03 done'
COMMIT;

BEGIN;
CREATE INDEX ON transactions_03 (cardmereq_key);
COMMIT;

BEGIN;
CREATE TABLE temp.transactions_03_subset AS
SELECT *
FROM transactions_03
WHERE EXISTS (
    SELECT 1
    FROM temp.equipments_subset a
    WHERE transactions_03.cardmereq_key = a.cardmereq_key
);

COMMIT;

-- smazani automatu ze supsetu equipments
delete from equipments_subset where cardmerc_unified_desc = 'ATM';

-- stores
create table stores as select distinct
	cardmereqh_short_name,
	cardmereqh_street_name,
	cardmereqh_street_number,
	cardmereqh_landreg_number,
	cardmereqh_zip_code,
	cardmereqh_geo_latitude,
	cardmereqh_geo_longitude 
  from 
	equipments_subset ;

alter table stores add column store_id serial;
drop sequence stores_store_id_seq cascade;

alter table equipments_subset add column store_id integer;
update equipments_subset a set store_id=b.store_id from stores b where a.cardmereqh_short_name=b.cardmereqh_short_name and a.cardmereqh_street_name=b.cardmereqh_street_name and a.cardmereqh_street_number=b.cardmereqh_street_number and a.cardmereqh_landreg_number=b.cardmereqh_landreg_number and a.cardmereqh_zip_code=b.cardmereqh_zip_code and a.cardmereqh_geo_latitude=b.cardmereqh_geo_latitude and a.cardmereqh_geo_longitude=b.cardmereqh_geo_longitude;

-- store_id do transaction
alter table transactions_subset add column store_id integer;
update transactions_subset a set store_id = b.store_id from equipments_subset b where a.CARDMEREQ_KEY=b.CARDMEREQ_KEY;


-- RFM
create table transactions_subset_klient as select a.*,b.pt_unified_key from transactions_subset a left join cards b on a.card_key=b.card_key;
alter table transactions_subset rename to transactions_subset_smaz2;
alter table transactions_subset_klient rename to transactions_subset;


create index on transactions_subset (card_key);
create index on transactions_subset (store_id);
create index on transactions_subset (pt_unified_key);


-- store
create table rfm_stores as select 
	pt_unified_key,
	min('04-30-2015'::date-CARDTR_TRANS_LOCAL_DATETIME::date) days_free,
	count(*) pocet,
	sum(CARDTR_AMOUNT_CZK::numeric) suma,
	store_id 
  from 
	transactions_subset 
  group by 
	pt_unified_key,store_id;


-- retezec
-- alter table equipments_subset add column retezec varchar(20);
-- update equipments_subset set retezec = 'penny' where cardmereqh_short_name ilike '%penny%';
-- update equipments_subset set retezec = 'billa' where cardmereqh_short_name ilike '%billa%';

alter table stores add column retezec varchar(20);
update stores set retezec = 'penny' where cardmereqh_short_name ilike '%penny%';
update stores set retezec = 'billa' where cardmereqh_short_name ilike '%billa%';

create table transactions_subset_retezec as select a.*,b.retezec from transactions_subset a left join stores b on a.store_id=b.store_id;
alter table transactions_subset rename to transactions_subset_smaz;
alter table transactions_subset_retezec rename to transactions_subset;

create index on transactions_subset (retezec);

create table rfm_retezec as select
        pt_unified_key,
        min('04-30-2015'::date-CARDTR_TRANS_LOCAL_DATETIME::date) days_free,
        count(*) pocet,
        sum(CARDTR_AMOUNT_CZK::numeric) suma,
        retezec
  from
        transactions_subset
  group by
        pt_unified_key,retezec;

-- pridani dat z parties k rfm
create table rfm_retezec_parties as select a.*,b.PSGEN_UNIFIED_DESC pohlavi,b.og_unified_desc segment from rfm_retezec a left join parties b on a.pt_unified_key=b.pt_unified_key;
drop table rfm_retezec;
alter table rfm_retezec_parties rename to rfm_retezec;

alter table rfm_retezec add column rfm_segment_vyse integer;
alter table rfm_retezec add column rfm_segment_frekvence integer;
alter table rfm_retezec add column rfm_segment_posledni integer;
alter table rfm_retezec add column rfm_segment_suma integer;

update rfm_retezec set rfm_segment_vyse = 1 where suma <= 1000;
update rfm_retezec set rfm_segment_vyse = 2 where suma > 1000 and suma <= 10000;
update rfm_retezec set rfm_segment_vyse = 3 where suma > 10000;


update rfm_retezec set rfm_segment_frekvence = 1 where pocet <= 4;
update rfm_retezec set rfm_segment_frekvence = 2 where pocet > 4 and pocet <= 16;
update rfm_retezec set rfm_segment_frekvence = 3 where pocet > 16;

update rfm_retezec set rfm_segment_posledni = 1 where days_free >= 30;
update rfm_retezec set rfm_segment_posledni = 2 where days_free < 30 and days_free >= 7;
update rfm_retezec set rfm_segment_posledni = 3 where days_free < 7;

update rfm_retezec set rfm_segment_suma = coalesce(rfm_segment_vyse,0)+coalesce(rfm_segment_frekvence,0)+coalesce(rfm_segment_posledni,0);















