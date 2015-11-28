BEGIN;
create view v_equipments_skiarealy as select cardmereq_key,cardmerc_desc,cardmereqh_short_name from equipments where cardmerc_desc = 'SLUZBY PRO VOLNY CAS-JINDE NESPECIFIK.' and (cardmereqh_short_name ilike '%lyž%' or cardmereqh_short_name ilike '%ski%' or cardmereqh_short_name ilike '%vlek%');
comment on view v_equipments_skiarealy is 'SKI arealy - pohled na skiarealy';
COMMIT;

BEGIN;
create view v_equipments_pokuty as select cardmereq_key,cardmerc_desc,cardmereqh_short_name from equipments where cardmerc_desc = 'POKUTY';
COMMIT;

