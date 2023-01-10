/* sed -i 's/,/./g' Transactions.tsv      (Linux only) */

/* import data from *.fsv file */
/* !!!!!!!!!!!!!!!!!!!BEFORE PROCEEDING WITH SCRIPT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
   PLEASE CHANGE THE ABSOLUTE PATH TO FSV FILES (LINE 10) */
----------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS path_name();
CREATE FUNCTION path_name() RETURNS varchar AS
$$
SELECT '/home/darika/retail/';
$$ LANGUAGE sql;


DROP TABLE IF EXISTS personal_data, cards, transactions, groups_sku,
    date_of_analysis_formation, sku, checks, stores CASCADE;

CREATE TABLE personal_data
(
    customer_id            bigint PRIMARY KEY,
    customer_name          varchar CHECK (customer_name ~ '^[A-ZА-Я][a-zа-яё -]+$'),
    customer_surname       varchar CHECK (customer_surname ~ '^[A-ZА-Я][a-zа-яё -]+$'),
    customer_primary_email varchar CHECK (customer_primary_email ~ '^[A-Za-z0-9._+%-]+@[A-Za-z0-9.-]+[.][A-Za-z]+$'),
    customer_primary_phone varchar CHECK (customer_primary_phone ~ '^[+][7][0-9]{10}')
);

CREATE TABLE cards
(
    customer_card_id bigint PRIMARY KEY,
    customer_id      bigint NOT NULL REFERENCES personal_data (customer_id)
);

CREATE TABLE groups_sku
(
    group_id   bigint PRIMARY KEY,
    group_name varchar CHECK (group_name ~ '^[A-ZА-Яa-zа-яё0-9 -\[\]\\\^\$\.\|\?\*\+\(\)]+$')
);

CREATE TABLE sku
(
    sku_id   bigint PRIMARY KEY,
    sku_name varchar CHECK (sku_name ~ '^[A-ZА-Яa-zа-яё0-9 -\[\]\\\^\$\.\|\?\*\+\(\)]+$'),
    group_id bigint NOT NULL REFERENCES groups_sku (group_id)
);

CREATE TABLE stores
(
    transaction_store_id bigint,
    sku_id               bigint REFERENCES sku (sku_id),
    sku_purchase_price   numeric CHECK (sku_purchase_price >= 0),
    sku_retail_price     numeric CHECK (sku_retail_price >= 0)
);

CREATE TABLE transactions
(
    transaction_id       bigint PRIMARY KEY,
    customer_card_id     bigint REFERENCES cards (customer_card_id),
    transaction_summ     numeric,
    transaction_datetime timestamp,
    transaction_store_id bigint
);

CREATE TABLE checks
(
    transaction_id bigint NOT NULL REFERENCES transactions (transaction_id),
    sku_id         bigint NOT NULL REFERENCES sku (sku_id),
    sku_amount     numeric,
    sku_summ       numeric,
    sku_summ_paid  numeric,
    sku_discount   numeric
);

CREATE TABLE date_of_analysis_formation
(
    analysis_formation timestamp
);

DROP PROCEDURE IF EXISTS import_data(varchar, char);
CREATE PROCEDURE import_data(tablename varchar, delimeter char)
AS
$$
BEGIN
    EXECUTE FORMAT('COPY %s FROM %L DELIMITER %L CSV', tablename,
                   (SELECT path_name() || 'datasets/' || tablename || '.tsv'), delimeter);
END;
$$ LANGUAGE plpgsql;

DROP PROCEDURE IF EXISTS import_data_mini(varchar, char);
CREATE PROCEDURE import_data_mini(tablename varchar, delimeter char)
AS
$$
BEGIN
    EXECUTE FORMAT('COPY %s FROM %L DELIMITER %L CSV', tablename,
                   (SELECT path_name() || 'datasets/' || tablename || '_Mini.tsv'), delimeter);
END;
$$ LANGUAGE plpgsql;

DROP PROCEDURE IF EXISTS export_data(varchar, char);
CREATE PROCEDURE export_data(tablename varchar, delimeter char)
AS
$$
BEGIN
    EXECUTE FORMAT('COPY %s TO %L DELIMITER %L CSV HEADER', tablename,
                   (SELECT path_name() || 'src/export/' || tablename || '.fsv'), delimeter);
END;
$$ LANGUAGE plpgsql;


SET datestyle TO iso, DMY;
CALL import_data('Date_Of_Analysis_Formation', E'\t');

/* import_mini */

-- call import_data_mini('Personal_Data', E'\t');
-- call import_data_mini('Cards', E'\t');
-- call import_data_mini('Transactions', E'\t');
-- call import_data_mini('Groups_SKU', E'\t');
-- call import_data_mini('SKU', E'\t');
-- call import_data_mini('Checks', E'\t');
-- call import_data_mini('Stores', E'\t');

/* import */

CALL import_data('Personal_Data', E'\t');
CALL import_data('Cards', E'\t');
CALL import_data('Transactions', E'\t');
CALL import_data('Groups_SKU', E'\t');
CALL import_data('SKU', E'\t');
CALL import_data('Checks', E'\t');
CALL import_data('Stores', E'\t');

/* export */

CALL export_data('personal_data', E'\t');
CALL export_data('cards', E'\t');
CALL export_data('transactions', E'\t');
CALL export_data('groups_sku', E'\t');
CALL export_data('sku', E'\t');
CALL export_data('checks', E'\t');
CALL export_data('stores', E'\t');
CALL export_data('date_of_analysis_formation', E'\t');