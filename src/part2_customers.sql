DROP FUNCTION IF EXISTS checks_cte() CASCADE;
CREATE FUNCTION checks_cte()
    RETURNS table
            (
                customer_id                    bigint,
                customer_average_check         decimal,
                customer_average_check_segment varchar(30)
            )
AS
$$
DECLARE
    total_lines numeric := (SELECT COUNT(*)
                            FROM personal_data);
BEGIN
    RETURN QUERY
        WITH average_checks AS (SELECT pd.customer_id,
                                       (CASE
                                            WHEN COUNT(t.transaction_summ) = 0 THEN 0
                                            ELSE ROUND(SUM(t.transaction_summ) / COUNT(t.transaction_summ), 2) END)
                                           AS customer_average_check
                                FROM personal_data pd
                                         LEFT JOIN cards c ON pd.customer_id = c.customer_id
                                         LEFT JOIN transactions t ON c.customer_card_id = t.customer_card_id
                                GROUP BY 1
                                ORDER BY 2 DESC),
             check_segments AS (SELECT *,
                                       (CASE
                                            WHEN ROW_NUMBER() OVER ()
                                                <= ROUND((total_lines * 0.1), 0) THEN 'High'::varchar(30)
                                            WHEN ROW_NUMBER() OVER ()
                                                <= ROUND((total_lines * 0.35), 0) THEN 'Medium'::varchar(30)
                                            ELSE 'Low'::varchar(30) END)
                                           AS customer_average_check_segment
                                FROM average_checks)
        SELECT *
        FROM check_segments;
END;
$$ LANGUAGE plpgsql;



DROP FUNCTION IF EXISTS transactions_cte() CASCADE;
CREATE FUNCTION transactions_cte()
    RETURNS table
            (
                customer_id                bigint,
                customer_frequency         decimal,
                customer_frequency_segment varchar(30),
                customer_inactive_period   decimal
            )
AS
$$
DECLARE
    total_lines numeric := (SELECT COUNT(*)
                            FROM personal_data);
BEGIN
    RETURN QUERY
        WITH max_min_transactions AS (SELECT pd.customer_id,
                                             MAX(t.transaction_datetime)      AS latest,
                                             MIN(t.transaction_datetime)      AS earliest,
                                             COUNT(DISTINCT t.transaction_id) AS t_count
                                      FROM personal_data pd
                                               LEFT JOIN cards c ON pd.customer_id = c.customer_id
                                               LEFT JOIN transactions t ON c.customer_card_id = t.customer_card_id
                                      GROUP BY 1),
             frequency AS (SELECT mm.customer_id,
                                  (CASE
                                       WHEN mm.t_count = 0 THEN 0
                                       ELSE ROUND(((EXTRACT(EPOCH FROM NOW()) - EXTRACT(EPOCH FROM mm.latest))
                                           / (60 * 60 * 24))::decimal, 2) END)              AS customer_inactive_period,
                                  (CASE
                                       WHEN mm.t_count = 0 THEN 0
                                       ELSE ROUND(((EXTRACT(EPOCH FROM mm.latest) - EXTRACT(EPOCH FROM mm.earliest))
                                           / mm.t_count / (60 * 60 * 24))::decimal, 2) END) AS customer_frequency
                           FROM max_min_transactions mm
                           ORDER BY 3 ASC),
             t_segments AS (SELECT f.customer_id,
                                   f.customer_frequency,
                                   (CASE
                                        WHEN ROW_NUMBER() OVER ()
                                            <= ROUND((total_lines * 0.1), 0) THEN 'Often'::varchar(30)
                                        WHEN ROW_NUMBER() OVER ()
                                            <= ROUND((total_lines * 0.35), 0) THEN 'Occasionally'::varchar(30)
                                        ELSE 'Rarely'::varchar(30) END)
                                       AS customer_frequency_segment,
                                   f.customer_inactive_period
                            FROM frequency f)

        SELECT *
        FROM t_segments;
END;
$$ LANGUAGE plpgsql;


DROP VIEW IF EXISTS customers;
CREATE VIEW customers AS
WITH main AS (SELECT ch.customer_id,
                     ch.customer_average_check,
                     ch.customer_average_check_segment,
                     q.customer_frequency,
                     q.customer_frequency_segment,
                     q.customer_inactive_period,
                     (CASE
                          WHEN q.customer_frequency = 0 THEN 0
                          ELSE ROUND(q.customer_inactive_period / q.customer_frequency, 2) END) AS customer_churn_rate
              FROM checks_cte() ch
                       JOIN
                       (SELECT * FROM transactions_cte()) q
                       ON ch.customer_id = q.customer_id),
     churn_probability AS (SELECT *,
                                  (CASE
                                       WHEN customer_churn_rate <= 2 THEN 'Low'
                                       WHEN customer_churn_rate <= 5 THEN 'Medium'
                                       ELSE 'High' END) AS customer_churn_segment
                           FROM main)

SELECT *
FROM churn_probability;

-- SELECT pd.customer_id,
--        t.customer_card_id,
--        t.transaction_datetime
-- FROM personal_data pd
--          LEFT JOIN cards c ON pd.customer_id = c.customer_id
--          LEFT JOIN transactions t ON c.customer_card_id = t.customer_card_id
-- WHERE pd.customer_id = 652;