DROP VIEW IF EXISTS main CASCADE;
CREATE VIEW main AS
SELECT cd.customer_id
     , tr.transaction_id
     , tr.transaction_datetime
     , ch.sku_id
     , sku.group_id
     , ch.sku_amount
     , ch.sku_summ
     , ch.sku_summ_paid
     , ch.sku_discount
     , st.sku_purchase_price
     , st.sku_purchase_price * ch.sku_amount AS sku_cost
     , st.sku_retail_price
FROM transactions AS tr
         JOIN cards AS cd
              ON cd.customer_card_id = tr.customer_card_id
         JOIN checks AS ch
              ON ch.transaction_id = tr.transaction_id
         JOIN sku
              ON sku.sku_id = ch.sku_id
         JOIN stores AS st
              ON st.transaction_store_id = tr.transaction_store_id
                  AND st.sku_id = sku.sku_id;

/* Purchase history View */

DROP MATERIALIZED VIEW IF EXISTS purchase_history;
CREATE MATERIALIZED VIEW purchase_history AS
SELECT customer_id                  AS "Customer_ID"
     , transaction_id               AS "Transaction_ID"
     , transaction_datetime         AS "Transaction_DateTime"
     , group_id                     AS "Group_ID"
     , ROUND(SUM(sku_cost), 2)      AS "Group_Cost"
     , ROUND(SUM(sku_summ), 2)      AS "Group_Summ"
     , ROUND(SUM(sku_summ_paid), 2) AS "Group_Summ_Paid"
FROM main
GROUP BY customer_id
       , transaction_id
       , transaction_datetime
       , group_id;

/* Periods View */

DROP MATERIALIZED VIEW IF EXISTS periods;
CREATE MATERIALIZED VIEW periods AS
SELECT customer_id                                                                          AS "Customer_ID"
     , group_id                                                                             AS "Group_ID"
     , MIN(transaction_datetime)                                                            AS "First_Group_Purchase_Date"
     , MAX(transaction_datetime)                                                            AS "Last_Group_Purchase_Date"
     , COUNT(*)                                                                             AS "Group_Purchase"
     , ((MAX(transaction_datetime)::date - MIN(transaction_datetime)::date) + 1) / COUNT(*) AS "Group_Frequency"
     , ROUND(MIN(sku_discount / sku_summ), 2)                                               AS "Group_Min_Discount"
FROM main
GROUP BY customer_id
       , group_id;



DROP MATERIALIZED VIEW IF EXISTS affinity_index;
CREATE MATERIALIZED VIEW affinity_index AS
SELECT p."Customer_ID"                                    AS customer_id
     , p."Group_ID"                                       AS group_id
     , ROUND((p."Group_Purchase"::numeric / COUNT(*)), 2) AS group_affinity_index
FROM periods AS p
         JOIN purchase_history AS ph
              ON ph."Customer_ID" = p."Customer_ID"
                  AND ph."Transaction_DateTime" >= p."First_Group_Purchase_Date"
                  AND ph."Transaction_DateTime" <= p."Last_Group_Purchase_Date"
GROUP BY p."Customer_ID"
       , p."Group_ID"
       , p."Group_Purchase";

SELECT *
FROM affinity_index
WHERE customer_id = 1
ORDER BY 1, 2;