
/* sed -i 's/,/./g' Transactions.tsv      (Linux only) */

drop table if exists personal_data, cards, transactions, groups_sku, 
					date_of_analysis_formation, sku, checks, stores cascade;

create table personal_data (
    customer_id bigint primary key,
	customer_name varchar not null,
	customer_surname varchar not null,
	customer_primary_email varchar not null,
	customer_primary_phone varchar not null
);

create table cards (
    customer_card_id bigint primary key,
	customer_id bigint not null references personal_data(customer_id)
);

create table transactions (
    transaction_id bigint primary key,
	customer_card_id bigint references cards(customer_card_id),
	transaction_summ numeric,
	transaction_datetime timestamp,
    transaction_store_id bigint
);

create table groups_sku (
    group_id bigint primary key,
    group_name varchar
);

create table sku (
    sku_id bigint primary key,
	sku_name varchar,
    group_id bigint not null references groups_sku(group_id)
);

create table checks (
    transaction_id bigint not null references transactions(transaction_id),
	sku_id bigint not null references sku(sku_id),
	sku_amount numeric,
	sku_summ numeric,
	sku_summ_paid numeric,
	sku_discount numeric
);

create table stores (
    transaction_store_id bigint,
	sku_id bigint references sku(sku_id),
	sku_purchase_price numeric,
	sku_retail_price numeric
);

create table date_of_analysis_formation (
    analysis_formation timestamp
);

/* import data from *.fsv file */
/* !!!!!!!!!!!!!!!!!!!BEFORE PROCEEDING WITH SCRIPT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
   PLEASE CHANGE THE ABSOLUTE PATH TO FSV FILES (LINES 14 & 34) */
----------------------------------------------------------------------------------
drop function if exists path_name();
create function path_name() returns varchar as
$$
select '/Users/alyssaiv/SQL3_RetailAnalitycs_v1.0-0/datasets/';
$$ language sql;

drop procedure if exists import_data(varchar, char);
create procedure import_data(tablename varchar, delimeter char)
as $$
begin
    execute format('COPY %s FROM %L DELIMITER %L CSV', tablename, (select path_name() || tablename || '.tsv'),
                   delimeter);
end;
$$ language plpgsql;

call import_data('personal_data', E'\t');
call import_data('cards', E'\t');

set datestyle to iso, DMY; 
--  Query:select '12/19/2016'::date Output: "2016-12-19"

call import_data('transactions', E'\t');
call import_data('groups_sku', E'\t');
call import_data('date_of_analysis_formation', E'\t');
call import_data('sku', E'\t');
call import_data('checks', E'\t');
call import_data('stores', E'\t');