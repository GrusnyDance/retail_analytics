/* sed -i 's/,/./g' Transactions.tsv      (Linux only) */

/* import data from *.fsv file */
/* !!!!!!!!!!!!!!!!!!!BEFORE PROCEEDING WITH SCRIPT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
   PLEASE CHANGE THE ABSOLUTE PATH TO FSV FILES (LINE 10) */
----------------------------------------------------------------------------------
drop function if exists path_name();
create function path_name() returns varchar as
$$
select '/home/natalia/School/SQL3_RetailAnalitycs_v1.0-0/';
$$ language sql;


drop table if exists personal_data, cards, transactions, groups_sku, 
					date_of_analysis_formation, sku, checks, stores cascade;

create table personal_data (
    customer_id bigint primary key,
	customer_name varchar check (customer_name ~ '^[A-ZА-Я][a-zа-яё -]+$'),
	customer_surname varchar check (customer_surname ~ '^[A-ZА-Я][a-zа-яё -]+$'),
	customer_primary_email varchar check (customer_primary_email ~ '^[A-Za-z0-9._+%-]+@[A-Za-z0-9.-]+[.][A-Za-z]+$'),
	customer_primary_phone varchar check (customer_primary_phone ~ '^[+][7][0-9]{10}')
);

create table cards (
    customer_card_id bigint primary key,
	customer_id bigint not null references personal_data(customer_id)
);

create table groups_sku (
    group_id bigint primary key,
    group_name varchar check (group_name ~ '^[A-ZА-Яa-zа-яё0-9\[\]\\\^\$\.\|\?\*\+\(\)]+$')
);

create table sku (
    sku_id bigint primary key,
	sku_name varchar check (sku_name ~ '^[A-ZА-Яa-zа-яё0-9\[\]\\\^\$\.\|\?\*\+\(\)]+$'),
    group_id bigint not null references groups_sku(group_id)
);

create table stores (
    transaction_store_id bigint,
	sku_id bigint references sku(sku_id),
	sku_purchase_price numeric check (sku_purchase_price >= 0),
	sku_retail_price numeric check (sku_retail_price >= 0)
);

create table transactions (
    transaction_id bigint primary key,
	customer_card_id bigint references cards(customer_card_id),
	transaction_summ numeric,
	transaction_datetime timestamp,
    transaction_store_id bigint
);

create table checks (
    transaction_id bigint not null references transactions(transaction_id),
	sku_id bigint not null references sku(sku_id),
	sku_amount numeric,
	sku_summ numeric,
	sku_summ_paid numeric,
	sku_discount numeric
);

create table date_of_analysis_formation (
    analysis_formation timestamp
);



drop procedure if exists import_data(varchar, char);
create procedure import_data(tablename varchar, delimeter char)
as $$
begin
    execute format('COPY %s FROM %L DELIMITER %L CSV', tablename, 
	(select path_name() ||'datasets/'|| tablename || '.tsv'), delimeter);
end;
$$ language plpgsql;

drop procedure if exists import_data_mini(varchar, char);
create procedure import_data_mini(tablename varchar, delimeter char)
as $$
begin
    execute format('COPY %s FROM %L DELIMITER %L CSV', tablename, 
	(select path_name() ||'datasets/'|| tablename || '_mini.tsv'), delimeter);
end;
$$ language plpgsql;

drop procedure if exists export_data(varchar, char);
create procedure export_data(tablename varchar, delimeter char)
as $$
begin
    execute format('COPY %s TO %L DELIMITER %L CSV HEADER', tablename, 
	(select path_name() ||'src/export/'|| tablename || '.fsv'), delimeter);
end;
$$ language plpgsql;


set datestyle to iso, DMY; 
call import_data('date_of_analysis_formation', E'\t');

/* import_mini */

call import_data_mini('personal_data', E'\t');
call import_data_mini('cards', E'\t');
call import_data_mini('transactions', E'\t');
call import_data_mini('groups_sku', E'\t');
call import_data_mini('sku', E'\t');
call import_data_mini('checks', E'\t');
call import_data_mini('stores', E'\t');

/* import */

-- call import_data('personal_data', E'\t');
-- call import_data('cards', E'\t');
-- call import_data('transactions', E'\t');
-- call import_data('groups_sku', E'\t');
-- call import_data('sku', E'\t');
-- call import_data('checks', E'\t');
-- call import_data('stores', E'\t');

/* export */

call export_data('personal_data', E'\t');
call export_data('cards', E'\t');
call export_data('transactions', E'\t');
call export_data('groups_sku', E'\t');
call export_data('sku', E'\t');
call export_data('checks', E'\t');
call export_data('stores', E'\t');



--  SELECT SESSION_USER, CURRENT_USER;