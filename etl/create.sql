BEGIN;

DROP TABLE IF EXISTS accounts CASCADE;
CREATE TABLE accounts (
  record_number text,
  effective_date text,
  acc_key text,
  acch_open_date text,
  acch_close_date text,
  acch_close_flag text,
  acctp_key text,
  acctp_source_id text,
  acctp_desc text,
  prod_source_id text,
  prod_source_name text,
  orgh_unified_id text,
  orgh_unified_desc text,
  orgh_district_code3 text,
  orgh_microregion_id text,
  orgh_code text
);

DROP TABLE IF EXISTS cards CASCADE;
CREATE TABLE cards (
record_number text,
effective_date text,
card_key text,
pt_unified_key text,
acc_key text,
cardh_issue_reason_code text,
cardh_marketing_action_code text,
cardh_automatic_issue_flag text,
cardh_suppl_flag text,
cardh_contactless_flag text,
cardh_individual_design_flag text,
cardh_deleted_flag text,
cardtp_bin text,
cardtp_desc text,
cardtp_crdr_flag text,
cardptp_source_id text,
cardptp_desc text,
cardst_active_flag text,
cardst_source_id text,
cardst_desc text,
cardblc_source_id text,
cardblc_desc text
);

DROP TABLE IF EXISTS transactions CASCADE;
CREATE TABLE transactions (
  record_number text,
  effective_date_from text,
  effective_date_to text,
  cardtr_key text,
  card_key text,
  cardmereq_key text,
  cardmer_key text,
  cardtr_trans_local_datetime text,
  cardtr_process_date text,
  cardtr_amount_czk text,
  cardtrsc_source_id text,
  cardtrsc_desc text,
  cardtrtp_source_id text,
  cardtrtp_desc text,
  cardaurs_source_id text,
  cardaurs_desc text,
  cs_card_flag text,
  cs_cardmereqh_flag text,
  cs_cardmer_flag text
);

DROP TABLE IF EXISTS equipments CASCADE;
CREATE TABLE equipments (
  record_number text,
  effective_date text,
  cardmereq_key text,
  cardmereqh_source_id text,
  cardmeretp_source_id text,
  cardmeretp_desc text,
  cardmerc_source_id text,
  cardmerc_desc text,
  cardmerc_unified_id text,
  cardmerc_unified_desc text,
  cardmereqh_short_name text,
  cardmereqh_street_name text,
  cardmereqh_street_number text,
  cardmereqh_landreg_number text,
  cardmereqh_zip_code text,
  cardmereqh_geo_latitude text,
  cardmereqh_geo_longitude text
);

DROP TABLE IF EXISTS merchants CASCADE;
CREATE TABLE merchants (
record_number text,
effective_date text,
cardmer_key text,
cardmerh_source_id text,
cardmerh_type text,
cardmerh_start_act_date text,
cardmerh_end_act_date text
);


DROP TABLE IF EXISTS parties CASCADE;
CREATE TABLE parties (
record_number text,
effective_date text,
pt_unified_key text,
pttp_unified_id text,
pttp_unified_desc text,
psgen_unified_id text,
psgen_unified_desc text,
og_unified_id text,
og_unified_desc text,
sb_unified_id text,
sb_unified_desc text,
active_b24_internetbank_flag text,
active_s24_internetbank_flag text,
active_telebank_flag text,
active_gsmbank_flag text,
active_mobilebank_flag text,
orgh_unified_id text,
orgh_unified_desc text,
orgh_district_code3 text,
orgh_microregion_id text,
orgh_code text,
party_address_type_id text,
party_address_zip_code text,
party_address_geo_latitude text,
party_address_geo_longitude text
);
COMMIT;
BEGIN;
\copy equipments FROM '../DATAFEST_EQUIPMENTS.csv' WITH CSV HEADER DELIMITER '|' QUOTE '^';
\e 'equipments done'
COMMIT;
BEGIN;
\copy transactions FROM '../DATAFEST_TRANSACTIONS.csv' WITH CSV HEADER DELIMITER '|' QUOTE '^';
\e 'transactions done'
COMMIT;
BEGIN;
\copy merchants FROM '../DATAFEST_MERCHANTS.csv' WITH CSV HEADER DELIMITER '|' QUOTE '^';
\e 'merchants done'
COMMIT;
BEGIN;
\copy cards FROM '../DATAFEST_CARDS.csv' WITH CSV HEADER DELIMITER '|' QUOTE '^';
\e 'cards done'
COMMIT;
BEGIN;
\copy parties FROM '../DATAFEST_PARTIES.csv' WITH CSV HEADER DELIMITER '|' QUOTE '^';
\e 'parties done'
COMMIT;
BEGIN;
\copy accounts FROM '../DATAFEST_ACCOUNTS.csv' WITH CSV HEADER DELIMITER '|' QUOTE '^';
\e 'accounts done'
COMMIT;
