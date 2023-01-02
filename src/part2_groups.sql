DROP VIEW IF EXISTS basic_transactions CASCADE;
CREATE VIEW basic_transactions AS
SELECT pd.customer_id,
       t.transaction_id,
       t.transaction_datetime,
       s.group_id,
       p."First_Group_Purchase_Date",
       p."Last_Group_Purchase_Date",
       p."Group_Purchase"
FROM personal_data pd
         JOIN cards c ON pd.customer_id = c.customer_id
         JOIN transactions t ON c.customer_card_id = t.customer_card_id
         JOIN checks ch ON t.transaction_id = ch.transaction_id
         JOIN sku s ON ch.sku_id = s.sku_id
         JOIN periods p ON pd.customer_id = p."Customer_ID" AND s.group_id = p."Group_ID"
ORDER BY 1, 4;

DROP VIEW IF EXISTS affinity CASCADE;
CREATE VIEW affinity AS
WITH main AS (SELECT bt.*,
                     (SELECT COUNT(DISTINCT b.transaction_id)
                      FROM basic_transactions b
                      WHERE b.customer_id = bt.customer_id
                        AND (b.transaction_datetime BETWEEN bt."First_Group_Purchase_Date"
                          AND bt."Last_Group_Purchase_Date")) count_tr
              FROM basic_transactions bt),
     aff AS (SELECT DISTINCT m.customer_id,
                             m.group_id,
                             ROUND(m."Group_Purchase"::decimal / m.count_tr::decimal, 2) group_affinity_index
             FROM main m)
SELECT *
FROM aff;

SELECT *
FROM affinity
WHERE customer_id = 1;



--
-- DROP VIEW IF EXISTS affinity CASCADE;
-- CREATE VIEW affinity AS
-- WITH main AS (SELECT bt.customer_id,
--                      bt.group_id,
--                      bt."Group_Purchase",
--                      COUNT(DISTINCT bt.transaction_id)
-- --                      (SELECT COUNT(DISTINCT b.transaction_id)
-- --                       FROM basic_transactions b
-- --                       WHERE b.customer_id = bt.customer_id
-- --                         AND (b.transaction_datetime BETWEEN bt."First_Group_Purchase_Date"
-- --                           AND bt."Last_Group_Purchase_Date")) count_tr
--               FROM basic_transactions bt
--                        JOIN basic_transactions btr ON
--                           bt.customer_id = btr.customer_id AND
--                           btr.transaction_datetime BETWEEN bt."First_Group_Purchase_Date"
--                               AND bt."Last_Group_Purchase_Date"
--               GROUP BY 1, 2, 3)
-- --      aff AS (SELECT m.customer_id,
-- --                     m.group_id,
-- --                     ROUND(m."Group_Purchase"::decimal / m.count_tr::decimal, 2) group_affinity_index
-- --              FROM main m)
-- SELECT *
-- FROM main;
--
--
-- DROP MATERIALIZED VIEW IF EXISTS groups;
-- CREATE MATERIALIZED VIEW groups AS
-- SELECT *
-- FROM affinity;


--
-- SELECT personal_data.customer_id,
--        t.transaction_datetime,
--        t.transaction_id
-- FROM personal_data
--          JOIN cards c ON personal_data.customer_id = c.customer_id
--          JOIN transactions t ON c.customer_card_id = t.customer_card_id
-- WHERE personal_data.customer_id = 1;
