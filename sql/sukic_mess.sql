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

-- smazani automatu ze subsetu equipments
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

alter table rfm_retezec add column kod_zsj_d varchar(7);
update rfm_retezec a set kod_zsj_d=b.kod_zsj_d from parties_subset b where a.pt_unified_key=b.pt_unified_key;

--rename column rfm_retezec => zakaznici
create table zakaznici as select pt_unified_key customer_id,pohlavi,days_free posledni_nakup,segment,rfm_segment_vyse,rfm_segment_frekvence,rfm_segment_posledni,rfm_segment_suma rfm_suma,retezec,kod_zsj_d kod from rfm_retezec ;

-- top, bezny, umirajicia a mrtvy klient
create table rfm_stores_segments as select distinct store_id,null::int top_klient,null::int bezny_klient, null::int umirajici_klient, null::int mrtvy_klient from rfm_stores;


alter table rfm_stores add column rfm_segment_vyse integer;
alter table rfm_stores add column rfm_segment_frekvence integer;
alter table rfm_stores add column rfm_segment_posledni integer;
alter table rfm_stores add column rfm_segment_suma integer;

update rfm_stores set rfm_segment_vyse = 1 where suma <= 1000;
update rfm_stores set rfm_segment_vyse = 2 where suma > 1000 and suma <= 10000;
update rfm_stores set rfm_segment_vyse = 3 where suma > 10000;


update rfm_stores set rfm_segment_frekvence = 1 where pocet <= 4;
update rfm_stores set rfm_segment_frekvence = 2 where pocet > 4 and pocet <= 16;
update rfm_stores set rfm_segment_frekvence = 3 where pocet > 16;

update rfm_stores set rfm_segment_posledni = 1 where days_free >= 30;
update rfm_stores set rfm_segment_posledni = 2 where days_free < 30 and days_free >= 7;
update rfm_stores set rfm_segment_posledni = 3 where days_free < 7;

update rfm_stores set rfm_segment_suma = coalesce(rfm_segment_vyse,0)+coalesce(rfm_segment_frekvence,0)+coalesce(rfm_segment_posledni,0);
--
update rfm_stores_segments a set top_klient = b.pocet from (select count(*) pocet,store_id from rfm_stores where rfm_segment_suma >=8 group by store_id) b where a.store_id=b.store_id;
update rfm_stores_segments a set bezny_klient = b.pocet from (select count(*) pocet,store_id from rfm_stores where rfm_segment_suma in (6,7) group by store_id) b where a.store_id=b.store_id;
update rfm_stores_segments a set umirajici_klient = b.pocet from (select count(*) pocet,store_id from rfm_stores where rfm_segment_suma in (4,5) group by store_id) b where a.store_id=b.store_id;
update rfm_stores_segments a set mrtvy_klient = b.pocet from (select count(*) pocet,store_id from rfm_stores where rfm_segment_suma in (3) group by store_id) b where a.store_id=b.store_id;


-- pobocky
create table pobocky as select
   a.store_id,
   retezec nazev,
   city obec,
   cardmereqh_street_name ulice,
   cardmereqh_landreg_number cislo_popisne,
   top_klient,
   bezny_klient,
   umirajici_klient,
   mrtvy_klient, 
   coalesce(longitude,cardmereqh_geo_longitude) lon,
   coalesce(latitude,cardmereqh_geo_latitude) lat
from
   stores_web a 
left join
   rfm_stores_segments b 
      on a.store_id=b.store_id;

alter table pobocky alter COLUMN lon type double precision using lon::double precision;
alter table pobocky alter COLUMN lat type double precision using lat::double precision;

alter table pobocky add column geom geometry(point,3857);
update pobocky set geom = st_transform(st_setsrid(st_makepoint(lon,lat),4326),3857);

alter table pobocky add column obrat integer;
alter table pobocky add column zmena_obrat integer;

update pobocky a set obrat = b.suma from (select store_id,sum(suma_plateb) suma from transactions_records_agg group by store_id) b where a.store_id=b.s
tore_id;

update pobocky a set zmena_obrat = b.suma from (select store_id,sum(suma_plateb_4)-sum(suma_plateb_3) suma from transactions_records_agg group by store_id) b where a.store_id=b.store_id;

alter table pobocky add column poradi integer;
update pobocky a set poradi = b.poradi from (select store_id,obrat,rank() over (order by obrat desc) as poradi from pobocky order by obrat desc) b where a.store_id = b.store_id;


alter table pobocky add column prum_nakup integer;
update pobocky a set prum_nakup = b.prumer from (select sum(suma_plateb)/sum(pocet_plateb) prumer,store_id from transactions_records_agg group by store_id) b where a.store_id=b.store_id;

alter table pobocky add column pocet_klientu integer;
alter table pobocky add column pocet_transakci integer;
alter table pobocky add column potencial int;


update pobocky a set pocet_klientu=b.pocet from (select store_id,sum(pocet_klientu) pocet from transactions_records_agg group by store_id) b where a.store_id=b.store_id;
update pobocky a set pocet_transakci=b.pocet from (select store_id,sum(pocet_plateb) pocet from transactions_records_agg group by store_id) b where a.store_id=b.store_id;

alter table pobocky add column pocet_domacnosti int;
alter table pobocky add column pocet_obyvatel int;

update pobocky a set pocet_domacnosti = b.pocet from (select store_id,sum(pocet_domacnosti_zsj_d) pocet from transactions_records_agg where obrat_domacnost >=200 group by store_id) b where a.store_id=b.store_id;

update pobocky a set pocet_obyvatel = b.pocet from (select store_id,sum(pocet_obyvatel_zsj_d) pocet from transactions_records_agg where obrat_domacnost >=200 group by store_id) b where a.store_id=b.store_id;

update pobocky set potencial = pocet_domacnosti*20000;

alter table pobocky add column vylovenost double precision;
update pobocky set vylovenost = 1.0*obrat/potencial;

--prekryv -- presunuto do filter.sql

/*
alter table spadovky add column prekryv_billa int;
alter table spadovky add column prekryv_penny int;

alter table spadovky add column retezec varchar(20);
update spadovky a set retezec = b.retezec from stores b where a.store_id=b.store_id;

update spadovky  a set prekryv_billa = b.pocet from (select kod,count(*) pocet from spadovky where retezec = 'billa' and obrat_domacnost >=200 group by kod) b where a.kod=b.kod;
update spadovky  a set prekryv_penny = b.pocet from (select kod,count(*) pocet from spadovky where retezec = 'penny' and obrat_domacnost >=200 group by kod) b where a.kod=b.kod;

alter table pobocky add column vnitrni_konkurence int;
update pobocky a set vnitrni_konkurence = b.pocet from (select store_id,max(prekryv_billa) pocet from spadovky where retezec='billa' group by store_id) b where a.store_id=b.store_id;
update pobocky a set vnitrni_konkurence = b.pocet from (select store_id,max(prekryv_penny) pocet from spadovky where retezec='penny' group by store_id) b where a.store_id=b.store_id;
*/





