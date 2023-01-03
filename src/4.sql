DROP FUNCTION IF EXISTS average_count(int, decimal) CASCADE;
CREATE FUNCTION average_count(transactions_num int,
                              koeff_up decimal)
    RETURNS table
            (
                customer_id            bigint,
                required_check_measure decimal
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT DISTINCT q.customer_id,
                        ROUND((SUM(q.transaction_summ) OVER (PARTITION BY q.customer_id)) /
                              (COUNT(q.transaction_id) OVER (PARTITION BY q.customer_id)) * koeff_up, 2) avg_check
        FROM (SELECT pd.customer_id,
                     t.*,
                     ROW_NUMBER() OVER w AS check_count
              FROM personal_data pd
                       LEFT JOIN cards ON pd.customer_id = cards.customer_id
                       LEFT JOIN transactions t ON cards.customer_card_id = t.customer_card_id
                  WINDOW w AS (PARTITION BY pd.customer_id ORDER BY t.transaction_datetime DESC)
              ORDER BY 1) q
        WHERE q.check_count <= transactions_num
          AND q.transaction_id IS NOT NULL
        ORDER BY 1;
END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS average_period(date, date, decimal) CASCADE;
CREATE FUNCTION average_period(first_date date,
                               last_date date,
                               koeff_up decimal)
    RETURNS table
            (
                customer_id            bigint,
                required_check_measure decimal
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT DISTINCT pd.customer_id,
                        ROUND((SUM(q.transaction_summ) OVER (PARTITION BY pd.customer_id)) /
                              (COUNT(q.transaction_id) OVER (PARTITION BY pd.customer_id)) * koeff_up, 2) avg_check
        FROM personal_data pd
                 JOIN cards ON pd.customer_id = cards.customer_id
                 JOIN (SELECT *
                       FROM transactions
                       WHERE transaction_datetime BETWEEN first_date AND last_date) q
                      ON cards.customer_card_id = q.customer_card_id
        ORDER BY 1;
END;
$$ LANGUAGE plpgsql;



DROP FUNCTION IF EXISTS best_group(decimal, decimal, decimal) CASCADE;
CREATE FUNCTION best_group(max_churn decimal,
                           max_transitions_with_discount decimal,
                           max_margin_share decimal)
    RETURNS table
            (
                customer_id          bigint,
                group_name           varchar(30),
                offer_discount_depth decimal
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH main AS (SELECT grv."Customer_ID",
                             grv."Group_ID",
                             grv."Group_Affinity_Index",
                             (CASE
                                  WHEN ROUND(grv."Group_Minimum_Discount" / 0.05) * 0.05
                                      < grv."Group_Minimum_Discount"
                                      THEN ROUND(grv."Group_Minimum_Discount" / 0.05) * 0.05 + 0.05
                                  ELSE ROUND(grv."Group_Minimum_Discount" / 0.05) * 0.05 END)               AS round_discount,
                             grv."Group_Margin" * max_margin_share                                          AS max_margin, --max_margin_share,
                             grv."Group_Minimum_Discount",
                             ROW_NUMBER()
                             OVER (PARTITION BY grv."Customer_ID" ORDER BY grv."Group_Affinity_Index" DESC) AS check_priority
                      FROM groups_view grv
                      WHERE grv."Group_Churn_Rate" <= max_churn                        --max_churn
                        AND grv."Group_Discount_Share" < max_transitions_with_discount --max_transitions_with_discount;
                      ORDER BY 1),
             final_group AS (SELECT DISTINCT ON (m."Customer_ID") m."Customer_ID",
                                                                  gs.group_name::varchar(30),
                                                                  m.round_discount
                             FROM main m
                                      JOIN groups_sku gs ON m."Group_ID" = gs.group_id
                             WHERE m.round_discount < m.max_margin
                             ORDER BY m."Customer_ID", m.check_priority)
        SELECT *
        FROM final_group
        ORDER BY 1;

END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS average_check_growth(int, date, date, int,
                                             decimal, decimal, decimal, decimal) CASCADE;
CREATE FUNCTION average_check_growth(calc_method int DEFAULT 1,
                                     first_date date DEFAULT CURRENT_DATE,
                                     last_date date DEFAULT CURRENT_DATE,
                                     transactions_num int DEFAULT 5,
                                     koeff_up decimal DEFAULT 2,
                                     max_churn decimal DEFAULT 100,
                                     max_transitions_with_discount decimal DEFAULT 0.7,
                                     max_margin_share decimal DEFAULT 0.3)
    RETURNS table
            (
                customer_id            bigint,
                required_check_measure decimal,
                group_name             varchar(30),
                offer_discount_depth   decimal
            )
AS
$$
DECLARE
    upper_bound date := (SELECT MAX(transaction_datetime)
                         FROM transactions);
    lower_bound date := (SELECT MIN(transaction_datetime)
                         FROM transactions);
BEGIN
    IF last_date < first_date THEN
        last_date = upper_bound;
    END IF;
    IF last_date > upper_bound THEN
        last_date = upper_bound;
    END IF;
    IF first_date < lower_bound THEN
        first_date = lower_bound;
    END IF;

    IF calc_method = 1 THEN
        RETURN QUERY
            SELECT q1.customer_id,
                   q1.required_check_measure,
                   q2.group_name,
                   q2.offer_discount_depth
            FROM (SELECT *
                  FROM average_period(first_date, last_date, koeff_up)) q1
                     JOIN (SELECT * FROM best_group(max_churn, max_transitions_with_discount, max_margin_share)) q2
                          ON q1.customer_id = q2.customer_id;
    ELSIF calc_method = 2 THEN
        RETURN QUERY
            SELECT q1.customer_id,
                   q1.required_check_measure,
                   q2.group_name,
                   q2.offer_discount_depth
            FROM (SELECT *
                  FROM average_count(transactions_num, koeff_up)) q1
                     JOIN (SELECT * FROM best_group(max_churn, max_transitions_with_discount, max_margin_share)) q2
                          ON q1.customer_id = q2.customer_id;
    ELSE
        RAISE EXCEPTION 'Select 1 or 2 as calc method parameter';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- SELECT *
-- FROM average_check_growth(1, '2020-01-01'::date, '2021-01-01'::date);

SELECT *
FROM average_check_growth(2);