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

CREATE INDEX ON transactions(cardmereq_key);

CREATE TABLE temp.transactions_subset AS
SELECT *
FROM transactions
WHERE EXISTS (
    SELECT 1
    FROM temp.equipments_subset a
    WHERE transactions.cardmereq_key = a.cardmereq_key
);

COMMIT;
