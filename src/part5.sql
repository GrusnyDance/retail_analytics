DROP FUNCTION IF EXISTS frequency_of_visits(timestamp, timestamp, bigint, numeric, numeric, numeric);
CREATE FUNCTION frequency_of_visits(first_date timestamp DEFAULT '2023-02-01 08:24:58',
                                    last_date timestamp DEFAULT '2023-08-25 07:31:15',
                                    add_trans bigint DEFAULT 10,
                                    max_churn_index numeric DEFAULT 10,
                                    max_disc_share numeric DEFAULT 85,
                                    allow_margin_share numeric DEFAULT 50)
    RETURNS table
            (
                "Customer_ID"                 bigint,
                "Start_Date"                  timestamp,
                "End_Date"                    timestamp,
                "Required_Transactions_Count" numeric,
                "Group_Name"                  varchar,
                "Offer_Discount_Depth"        numeric
            )
AS
$$
DECLARE
    in_interv int := (last_date::date - first_date::date);
BEGIN
    RETURN QUERY
        WITH group_name AS (SELECT DISTINCT gv."Customer_ID"                                                             AS customer_id
                                          , FIRST_VALUE(gsku.group_name)
                                            OVER (PARTITION BY gv."Customer_ID" ORDER BY gv."Group_Affinity_Index" DESC) AS gr_name
                                          , FIRST_VALUE(gv."Group_Minimum_Discount")
                                            OVER (PARTITION BY gv."Customer_ID" ORDER BY gv."Group_Affinity_Index" DESC) AS depth_disc
                            FROM groups AS gv
                                     JOIN groups_sku AS gsku
                                          ON gsku.group_id = gv."Group_ID"
                            WHERE "Group_Churn_Rate" <= max_churn_index
                              AND "Group_Discount_Share" < max_disc_share::numeric / 100
                              AND "Group_Margin" * allow_margin_share::numeric / 100 > (CASE
                                                                                            WHEN ROUND(gv."Group_Minimum_Discount" / 0.05) *
                                                                                                 0.05 <
                                                                                                 gv."Group_Minimum_Discount"
                                                                                                THEN ROUND(gv."Group_Minimum_Discount" / 0.05) * 0.05 + 0.05
                                                                                            ELSE ROUND(gv."Group_Minimum_Discount" / 0.05) * 0.05 END))
        SELECT cs.customer_id
             , first_date
             , last_date
             , CASE
                   WHEN cs.customer_frequency = 0 THEN add_trans
                   ELSE (ROUND(in_interv::numeric / cs.customer_frequency, 0) + add_trans) END
             , gn.gr_name
             , CASE
                   WHEN ROUND(gn.depth_disc / 0.05) * 0.05 < gn.depth_disc
                       THEN (ROUND(gn.depth_disc / 0.05) * 0.05 + 0.05) * 100
                   ELSE (ROUND(gn.depth_disc / 0.05) * 0.05) * 100 END
        FROM customers AS cs
                 JOIN group_name AS gn
                      ON cs.customer_id = gn.customer_id;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM frequency_of_visits();
