DROP VIEW IF EXISTS sku_share_group;
CREATE VIEW sku_share_group AS
SELECT m1.group_id
     , m1.sku_id
     , m1.sku_qty::numeric / m2.gr_qty AS sku_share
FROM (SELECT group_id, sku_id, COUNT(DISTINCT transaction_id) AS sku_qty
      FROM main
      GROUP BY group_id, sku_id) AS m1
         JOIN (SELECT group_id, COUNT(DISTINCT transaction_id) AS gr_qty
               FROM main
               GROUP BY group_id) AS m2
              ON m1.group_id = m2.group_id;


DROP FUNCTION IF EXISTS cross_selling(int, numeric, numeric, numeric, numeric);
CREATE FUNCTION cross_selling(group_qty int DEFAULT 5,
                              max_churn_index numeric DEFAULT 25,
                              max_stability_index numeric DEFAULT 2.5,
                              max_sku_share numeric DEFAULT 70,
                              allow_margin_share numeric DEFAULT 60)
    RETURNS table
            (
                "Customer_ID"          bigint,
                "SKU_Name"             varchar,
                "Offer_Discount_Depth" numeric
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH step_one AS (SELECT t1."Customer_ID"           AS customer_id
                               , t1."Group_ID"              AS group_id
                               , cus.customer_primary_store AS c_store
                          FROM (SELECT *,
                                       ROW_NUMBER()
                                       OVER (PARTITION BY gv."Customer_ID" ORDER BY gv."Group_Affinity_Index" DESC) AS i
                                FROM groups AS gv
                                WHERE gv."Group_Churn_Rate" <= max_churn_index
                                  AND gv."Group_Stability_Index" < max_stability_index) AS t1
                                   JOIN customers AS cus
                                        ON cus.customer_id = t1."Customer_ID"
                          WHERE i <= group_qty),
             step_two AS (SELECT s1.customer_id
                               , s1.group_id
                               , s1.c_store
                               , temp.sku_id
                               , temp.diff_price
                               , temp.sku_retail_price
                          FROM step_one AS s1
                                   JOIN (SELECT st.transaction_store_id
                                              , st.sku_id
                                              , st.sku_retail_price
                                              , st.sku_retail_price - st.sku_purchase_price    AS diff_price
                                              , sku.group_id
                                              , ROW_NUMBER() OVER (PARTITION BY st.transaction_store_id, sku.group_id
                                  ORDER BY (st.sku_retail_price - st.sku_purchase_price) DESC) AS i
                                         FROM stores AS st
                                                  JOIN sku
                                                       ON st.sku_id = sku.sku_id) AS temp
                                        ON temp.transaction_store_id = s1.c_store
                                            AND temp.group_id = s1.group_id
                          WHERE temp.i = 1),
             step_three AS (SELECT s2.customer_id
                                 , s2.group_id
                                 , s2.c_store
                                 , s2.sku_id
                                 , (s2.diff_price * allow_margin_share::numeric / 100) / s2.sku_retail_price AS discount
                                 , CASE
                                       WHEN ROUND(p."Group_Min_Discount" / 0.05) * 0.05 < p."Group_Min_Discount"
                                           THEN (ROUND(p."Group_Min_Discount" / 0.05) * 0.05 + 0.05)
                                       ELSE (ROUND(p."Group_Min_Discount" / 0.05) * 0.05) END          AS min_discount
                            FROM step_two AS s2
                                     JOIN periods AS p
                                          ON s2.customer_id = p."Customer_ID"
                                              AND s2.group_id = p."Group_ID"
                                     JOIN sku_share_group AS ssg
                                          ON ssg.sku_id = s2.sku_id
                                              AND ssg.group_id = s2.group_id
                            WHERE sku_share <= max_sku_share::numeric / 100)
        SELECT customer_id
             , sku.sku_name
             , min_discount * 100
        FROM step_three AS s3
                 JOIN sku
                      ON sku.sku_id = s3.sku_id
        WHERE discount >= min_discount;

END;
$$ LANGUAGE plpgsql;

SELECT *
FROM cross_selling();
