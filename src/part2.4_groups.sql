DROP MATERIALIZED VIEW IF EXISTS affinity_index;
CREATE MATERIALIZED VIEW affinity_index AS
SELECT p."Customer_ID"                                                            AS customer_id
     , p."Group_ID"                                                               AS group_id
     , ROUND((p."Group_Purchase"::numeric / COUNT(DISTINCT "Transaction_ID")), 2) AS group_affinity_index
FROM periods AS p
         JOIN purchase_history AS ph
              ON ph."Customer_ID" = p."Customer_ID"
                  AND ph."Transaction_DateTime" >= p."First_Group_Purchase_Date"
                  AND ph."Transaction_DateTime" <= p."Last_Group_Purchase_Date"
GROUP BY p."Customer_ID"
       , p."Group_ID"
       , p."Group_Purchase";


DROP MATERIALIZED VIEW IF EXISTS churn_rate;
CREATE MATERIALIZED VIEW churn_rate AS
SELECT p."Customer_ID"                                                                      AS customer_id
     , p."Group_ID"                                                                         AS group_id
     , CASE
           WHEN p."Group_Frequency" = 0 THEN 0
           ELSE ROUND(((SELECT * FROM date_of_analysis_formation ORDER BY 1 DESC LIMIT 1)::date
               - p."Last_Group_Purchase_Date"::date)::numeric / p."Group_Frequency", 2) END AS churn_rate
FROM periods AS p
         JOIN purchase_history AS ph
              ON ph."Customer_ID" = p."Customer_ID"
GROUP BY p."Customer_ID"
       , p."Group_ID"
       , p."Last_Group_Purchase_Date"
       , p."Group_Frequency";


DROP MATERIALIZED VIEW IF EXISTS stability_index;
CREATE MATERIALIZED VIEW stability_index AS
WITH stability_temp AS (SELECT ph."Customer_ID"          AS customer_id
                             , ph."Group_ID"             AS group_id
                             , ph."Transaction_DateTime" AS tr_date
                             , COALESCE((ph."Transaction_DateTime"::date - LAG(ph."Transaction_DateTime")
                                                                           OVER (PARTITION BY ph."Customer_ID", ph."Group_ID" ORDER BY ph."Transaction_DateTime")::date),
                                        0)               AS intervals
                             , p."Group_Frequency"       AS gr_frequency
                        FROM purchase_history AS ph
                                 JOIN periods AS p
                                      ON p."Customer_ID" = ph."Customer_ID"
                                          AND p."Group_ID" = ph."Group_ID")
SELECT customer_id
     , group_id
     , ROUND(AVG(CASE
                     WHEN gr_frequency = 0 THEN 0
                     ELSE (CASE
                               WHEN (gr_frequency > intervals) THEN gr_frequency - intervals
                               ELSE intervals - gr_frequency END)::numeric / gr_frequency END), 2) AS stability_index
FROM stability_temp AS st
GROUP BY customer_id
       , group_id;


DROP MATERIALIZED VIEW IF EXISTS discount_share_min;
CREATE MATERIALIZED VIEW discount_share_min AS
WITH discount_transaction AS (SELECT m.customer_id
                                   , m.group_id
                                   , COUNT(DISTINCT transaction_id) AS qty_dis_tr
                              FROM main AS m
                              WHERE m.sku_discount > 0
                              GROUP BY m.customer_id
                                     , m.group_id)
SELECT p."Customer_ID"                                                    AS customer_id
     , p."Group_ID"                                                       AS group_id
     , ROUND(COALESCE(dt.qty_dis_tr, 0)::numeric / p."Group_Purchase", 2) AS group_discount_share
     , p."Group_Min_Discount"                                             AS group_min_discount
FROM discount_transaction AS dt
         RIGHT JOIN periods AS p
                    ON p."Customer_ID" = dt.customer_id
                        AND p."Group_ID" = dt.group_id;


DROP MATERIALIZED VIEW IF EXISTS group_average_discount;
CREATE MATERIALIZED VIEW group_average_discount AS
SELECT "Customer_ID"                                        AS customer_id
     , "Group_ID"                                           AS group_id
     , ROUND(SUM("Group_Summ_Paid") / SUM("Group_Summ"), 2) AS group_average_discount
FROM purchase_history
GROUP BY "Customer_ID"
       , "Group_ID";


DROP FUNCTION IF EXISTS group_margin(int, int) CASCADE;
CREATE FUNCTION group_margin(mode_margin int DEFAULT 3, in_value int DEFAULT 100)
    RETURNS table
            (
                customer_id  bigint,
                group_id     bigint,
                group_margin numeric
            )
AS
$$
BEGIN
    IF mode_margin = 1 THEN
        RETURN QUERY
            SELECT "Customer_ID"                                                                   AS customer_id
                 , "Group_ID"                                                                      AS group_id
                 , ROUND((SUM("Group_Summ_Paid") - SUM("Group_Cost")) / SUM("Group_Summ_Paid"), 2) AS group_margin
            FROM purchase_history
            WHERE "Transaction_DateTime"::date >=
                  ((SELECT * FROM date_of_analysis_formation ORDER BY 1 DESC LIMIT 1)::date - in_value)
            GROUP BY "Customer_ID"
                   , "Group_ID";
    ELSIF mode_margin = 2 THEN
        RETURN QUERY
            SELECT lph.customer_id
                 , lph.group_id
                 , ROUND(SUM(lph.margin / lph.group_paid), 2) AS group_margin
            FROM (SELECT "Customer_ID"                    AS customer_id
                       , "Group_ID"                       AS group_id
                       , "Group_Summ_Paid" - "Group_Cost" AS margin
                       , "Group_Summ_Paid"                AS group_paid
                  FROM purchase_history
                  ORDER BY "Transaction_DateTime" DESC
                  LIMIT 1000) AS lph
            GROUP BY lph.customer_id
                   , lph.group_id;
    ELSE
        RETURN QUERY
            SELECT "Customer_ID"                                                                   AS customer_id
                 , "Group_ID"                                                                      AS group_id
                 , ROUND((SUM("Group_Summ_Paid") - SUM("Group_Cost")) / SUM("Group_Summ_Paid"), 2) AS group_margin
            FROM purchase_history
            GROUP BY "Customer_ID"
                   , "Group_ID";
    END IF;
END;
$$ LANGUAGE plpgsql;


DROP VIEW IF EXISTS groups;
CREATE VIEW groups AS
SELECT gm.customer_id             AS "Customer_ID"
     , gm.group_id                AS "Group_ID"
     , ai.group_affinity_index    AS "Group_Affinity_Index"
     , cr.churn_rate              AS "Group_Churn_Rate"
     , si.stability_index         AS "Group_Stability_Index"
     , gm.group_margin            AS "Group_Margin"
     , dsm.group_discount_share   AS "Group_Discount_Share"
     , dsm.group_min_discount     AS "Group_Minimum_Discount"
     , gad.group_average_discount AS "Group_Average_Discount"
FROM group_margin() AS gm /* group_margin(mode, in_value) mode 1 for period, mode 2 for qty.
	                                                           in_value - qty of days or transaction */
         JOIN affinity_index AS ai
              ON ai.customer_id = gm.customer_id
                  AND ai.group_id = gm.group_id
         JOIN churn_rate AS cr
              ON cr.customer_id = gm.customer_id
                  AND cr.group_id = gm.group_id
         JOIN stability_index AS si
              ON si.customer_id = gm.customer_id
                  AND si.group_id = gm.group_id
         JOIN discount_share_min AS dsm
              ON dsm.customer_id = gm.customer_id
                  AND dsm.group_id = gm.group_id
         JOIN group_average_discount AS gad
              ON gad.customer_id = gm.customer_id
                  AND gad.group_id = gm.group_id;
