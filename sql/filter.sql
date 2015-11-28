/**
 * Filters equipments and transactions for Billa and Penny
 */
BEGIN;
DROP TABLE IF EXISTS temp.equipments_subset;
CREATE TABLE temp.equipments_subset AS
SELECT *
FROM equipments
WHERE cardmereqh_short_name ilike '%billa%'
    OR cardmereqh_short_name ilike '%penny%';

DELETE FROM temp.equipments_subset
WHERE cardmereqh_short_name IN ( -- false positives
    'ITS BILLA TRAVEL S.R.O.',
    'WWW.ITSBILLA.CZ',
    'PENNYBLACK'
);

ALTER TABLE temp.equipments_subset ADD COLUMN nazev text; -- make life easier

UPDATE temp.equipments_subset
SET nazev  = 'Penny'
WHERE cardmereqh_short_name ILIKE '%penny%';

UPDATE temp.equipments_subset
SET nazev = 'Billa'
WHERE cardmereqh_short_name ILIKE '%billa%';

ALTER TABLE temp.equipments_subset ADD PRIMARY KEY (cardmereq_key);

INSERT INTO temp.transactions_subset
SELECT *
FROM transactions_p20150430
WHERE EXISTS (
    SELECT 1
    FROM temp.equipments_subset a
    WHERE transactions_p20150430.cardmereq_key = a.cardmereq_key
);

CREATE TABLE parties_subset AS
    SELECT DISTINCT a.*, d.kod_zsj_d
    FROM parties a
    JOIN cards b ON (a.pt_unified_key = b.pt_unified_key)
    JOIN transactions_subset c ON (b.card_key = c.card_key)
    JOIN g_zsj_d_3857 d ON (ST_Intersects(a.wkb_geometry_2, d.geom));

DELETE FROM transactions_subset
WHERE NOT EXISTS (
    SELECT 1
    FROM equipments_subset a
    WHERE a.cardmereq_key = transactions_subset.cardmereq_key
);

DROP TABLE IF EXISTS transactions_records;
CREATE TABLE transactions_records AS
SELECT t.cardmereq_key,
    t.cardtr_trans_local_datetime,
    t.cardtr_amount_czk,
    p.psgen_unified_id,
    p.pttp_unified_desc,
    p.og_unified_desc,
    p.pt_unified_key,
    p.sb_unified_desc,
    p.kod_zsj_d,
    p.naz_zsj_d,
    z.pocet_obyvatel_zsj_d,
    z.pocet_domacnosti_zsj_d,
    s.store_id,
    c.card_key
FROM parties_subset p
JOIN cards c ON (p.pt_unified_key = c.pt_unified_key)
JOIN transactions_subset t ON (c.card_key = t.card_key)
JOIN equipments_subset e ON (t.cardmereq_key = e.cardmereq_key)
JOIN zsj_d_sldb11 z ON (p.kod_zsj_d = z.kod_zsj_d)
JOIN stores s ON (e.store_id = s.store_id);

DROP TABLE IF EXISTS transactions_records_agg;
CREATE TABLE transactions_records_agg AS
SELECT kod_zsj_d,
    naz_zsj_d,
    pocet_obyvatel_zsj_d,
    pocet_domacnosti_zsj_d,
    store_id,
    SUM(cardtr_amount_czk::float) suma_plateb,
    SUM(cardtr_amount_czk::float) / NULLIF(pocet_domacnosti_zsj_d, 0) obrat_domacnost,
    AVG(cardtr_amount_czk::float) prumerna_platba,
    SUM(CASE WHEN extract(month from cardtr_trans_local_datetime::timestamp) = 3 THEN cardtr_amount_czk::float ELSE 0 END) suma_plateb_3,
    SUM(CASE WHEN extract(month from cardtr_trans_local_datetime::timestamp) = 4 THEN cardtr_amount_czk::float ELSE 0 END) suma_plateb_4,
    COUNT(DISTINCT card_key) pocet_karet,
    COUNT(1) pocet_plateb,
    COUNT(DISTINCT pt_unified_key) pocet_klientu,
    SUM(zeny) pocet_zen,
    SUM(muzi) pocet_muzu,
    SUM(ofo) pocet_ofo,
    SUM(opo) pocet_opo,
    SUM(klient_standard) pocet_klient_standard,
    SUM(klient_plus) pocet_klient_plus,
    SUM(klient_premier) pocet_klient_premier,
    SUM(podnikatele) pocet_podnikatele,
    SUM(deti) deti,
    SUM(mladi_lide) mladi_lide,
    SUM(vysokoskolaci) vysokoskolaci,
    SUM(zamestnanci) zamestnanci,
    SUM(svobodna_povolani) svobodna_povolani,
    SUM(zamestnanci_cs) zamestnanci_cs,
    SUM(ostatni) ostatni,
    SUM(duchodci) duchodci
FROM (
    SELECT naz_zsj_d,
        kod_zsj_d,
        store_id,
        pocet_obyvatel_zsj_d,
        pocet_domacnosti_zsj_d,
        cardtr_amount_czk,
        CASE
            WHEN psgen_unified_id = 'Z'
            THEN 1
            ELSE 0
        END as zeny,
        CASE
            WHEN psgen_unified_id = 'M'
            THEN 1
            ELSE 0
        END as muzi,
        CASE
            WHEN pttp_unified_desc = 'FYZICKÁ OSOBA'
            THEN 1
            ELSE 0
        END as ofo,
        CASE
            WHEN pttp_unified_desc = 'PRÁVNICKÁ OSOBA'
            THEN 1
            ELSE 0
        END as opo,
        CASE
            WHEN og_unified_desc = 'STANDARD'
            THEN 1
            ELSE 0
        END as klient_standard,
        CASE
            WHEN og_unified_desc = 'PLUS'
            THEN 1
            ELSE 0
        END as klient_plus,
        CASE
            WHEN og_unified_desc = 'PREMIER'
            THEN 1
            ELSE 0
        END as klient_premier,
        CASE
            WHEN sb_unified_desc IN ('FK9_PODNIKATELÉ', 'SK9_PODNIKATELÉ')
            THEN 1
            ELSE 0
        END as podnikatele,
        CASE
            WHEN sb_unified_desc = 'SK1_DĚTI'
            THEN 1
            ELSE 0
        END as deti,
        CASE
            WHEN sb_unified_desc = 'SK2_MLADÍ LIDÉ'
            THEN 1
            ELSE 0
        END as mladi_lide,
        CASE
            WHEN sb_unified_desc = 'SK3_VYSOKOŠKOLÁCI'
            THEN 1
            ELSE 0
        END as vysokoskolaci,
        CASE
            WHEN sb_unified_desc = 'SK4_ZAMĚSTNANCI'
            THEN 1
            ELSE 0
        END as zamestnanci,
        CASE
            WHEN sb_unified_desc = 'SK5_SVOBODNÁ POVOLÁNÍ'
            THEN 1
            ELSE 0
        END as svobodna_povolani,
        CASE
            WHEN sb_unified_desc = 'SK6_ZAMĚSTNANCI FS ČS'
            THEN 1
            ELSE 0
        END as zamestnanci_cs,
        CASE
            WHEN sb_unified_desc = 'SK7_OSTATNÍ'
            THEN 1
            ELSE 0
        END as ostatni,
        CASE
            WHEN sb_unified_desc = 'SK8_DŮCHODCI'
            THEN 1
            ELSE 0
        END as duchodci,
        pt_unified_key,
        cardtr_trans_local_datetime,
        card_key
    FROM transactions_records
) a
GROUP BY naz_zsj_d, kod_zsj_d, pocet_obyvatel_zsj_d, pocet_domacnosti_zsj_d, store_id;

DROP TABLE IF EXISTS spadovky;
CREATE TABLE spadovky AS
SELECT geom,
    a.naz_zsj_d nazev,
    a.kod_zsj_d kod,
    pocet_domacnosti_zsj_d * 4 * 4000,
    suma_plateb / (NULLIF(pocet_domacnosti_zsj_d, 0) * 4 * 4000) vylovenost,
    suma_plateb obrat,
    store_id,
    prumerna_platba prumerny_nakup,
    pocet_karet,
    obrat_domacnost
FROM transactions_records_agg a
JOIN g_zsj_d_3857 b ON (a.kod_zsj_d = b.kod_zsj_d);

COMMIT;
