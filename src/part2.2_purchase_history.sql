DROP MATERIALIZED VIEW IF EXISTS main CASCADE;
CREATE MATERIALIZED VIEW main AS
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

DROP VIEW IF EXISTS purchase_history;
CREATE VIEW purchase_history AS
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

