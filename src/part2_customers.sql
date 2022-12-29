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


DROP FUNCTION IF EXISTS total_segment_cte() CASCADE;
CREATE FUNCTION total_segment_cte()
    RETURNS table
            (
                segment             bigint,
                average_check       varchar(30),
                frequency_purchases varchar(30),
                ch_probability      varchar(30)
            )
AS
$$
BEGIN
    DROP TABLE IF EXISTS temp_table;
    CREATE TABLE temp_table
    (
        segment             bigint,
        average_check       varchar(30),
        frequency_purchases varchar(30),
        ch_probability      varchar(30)
    );
    INSERT INTO temp_table VALUES (1, 'Low', 'Rarely', 'Low');
    INSERT INTO temp_table VALUES (2, 'Low', 'Rarely', 'Medium');
    INSERT INTO temp_table VALUES (3, 'Low', 'Rarely', 'High');
    INSERT INTO temp_table VALUES (4, 'Low', 'Occasionally', 'Low');
    INSERT INTO temp_table VALUES (5, 'Low', 'Occasionally', 'Medium');
    INSERT INTO temp_table VALUES (6, 'Low', 'Occasionally', 'High');
    INSERT INTO temp_table VALUES (7, 'Low', 'Often', 'Low');
    INSERT INTO temp_table VALUES (8, 'Low', 'Often', 'Medium');
    INSERT INTO temp_table VALUES (9, 'Low', 'Often', 'High');
    INSERT INTO temp_table VALUES (10, 'Medium', 'Rarely', 'Low');
    INSERT INTO temp_table VALUES (11, 'Medium', 'Rarely', 'Medium');
    INSERT INTO temp_table VALUES (12, 'Medium', 'Rarely', 'High');
    INSERT INTO temp_table VALUES (13, 'Medium', 'Occasionally', 'Low');
    INSERT INTO temp_table VALUES (14, 'Medium', 'Occasionally', 'Medium');
    INSERT INTO temp_table VALUES (15, 'Medium', 'Occasionally', 'High');
    INSERT INTO temp_table VALUES (16, 'Medium', 'Often', 'Low');
    INSERT INTO temp_table VALUES (17, 'Medium', 'Often', 'Medium');
    INSERT INTO temp_table VALUES (18, 'Medium', 'Often', 'High');
    INSERT INTO temp_table VALUES (19, 'Medium', 'Rarely', 'Low');
    INSERT INTO temp_table VALUES (20, 'High', 'Rarely', 'Medium');
    INSERT INTO temp_table VALUES (21, 'High', 'Rarely', 'High');
    INSERT INTO temp_table VALUES (22, 'High', 'Occasionally', 'Low');
    INSERT INTO temp_table VALUES (23, 'High', 'Occasionally', 'Medium');
    INSERT INTO temp_table VALUES (24, 'High', 'Occasionally', 'High');
    INSERT INTO temp_table VALUES (25, 'High', 'Often', 'Low');
    INSERT INTO temp_table VALUES (26, 'High', 'Often', 'Medium');
    INSERT INTO temp_table VALUES (27, 'High', 'Often', 'High');
    RETURN QUERY
        SELECT * FROM temp_table;
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
                           FROM main),
     last_segment AS (SELECT cp.*,
                             q.segment
                      FROM churn_probability cp
                               JOIN (SELECT * FROM total_segment_cte()) q
                                    ON cp.customer_average_check_segment = q.average_check AND
                                       cp.customer_frequency_segment = q.frequency_purchases AND
                                       cp.customer_churn_segment = q.ch_probability)

SELECT *
FROM churn_probability;

-- SELECT pd.customer_id,
--        t.customer_card_id,
--        t.transaction_datetime
-- FROM personal_data pd
--          LEFT JOIN cards c ON pd.customer_id = c.customer_id
--          LEFT JOIN transactions t ON c.customer_card_id = t.customer_card_id
-- WHERE pd.customer_id = 652;