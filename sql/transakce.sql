BEGIN;

DROP TABLE IF EXISTS transakce;
CREATE TABLE transakce AS
SELECT id,
    suma,
    CASE
        WHEN den = 0
            THEN 7
        ELSE den
    END den,
    hodina,
    cardtr_trans_local_datetime,
    customer_id,
    store_id
FROM (
    SELECT a.cardtr_key id,
        a.cardtr_amount_czk suma,
        extract(DOW FROM a.cardtr_trans_local_datetime::timestamp) den,
        extract(hour FROM a.cardtr_trans_local_datetime::timestamp) hodina,
        a.cardtr_trans_local_datetime,
        c.pt_unified_key customer_id,
        a.store_id
    FROM transactions_subset a
    JOIN cards c ON (a.card_key = c.card_key)
) a;

COMMIT;
