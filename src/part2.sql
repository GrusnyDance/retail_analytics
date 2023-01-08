drop materialized view if exists main cascade;
create materialized view main as
    select
		cd.customer_id
	  , tr.transaction_id
	  , tr.transaction_datetime
	  , ch.sku_id
	  , sku.group_id
	  , ch.sku_amount
	  , ch.sku_summ
	  , ch.sku_summ_paid
	  , ch.sku_discount
	  , st.sku_purchase_price
	  , st.sku_purchase_price * ch.sku_amount as sku_cost
	  , st.sku_retail_price
	from transactions as tr
	  join cards as cd
		on cd.customer_card_id = tr.customer_card_id
	  join checks as ch
		on ch.transaction_id = tr.transaction_id
	  join sku
		on sku.sku_id = ch.sku_id
	  join stores as st
		on st.transaction_store_id = tr.transaction_store_id
		and st.sku_id = sku.sku_id;

/* Purchase history View */

drop view if exists purchase_history;
create view purchase_history as
    select
    customer_id as "Customer_ID"
  , transaction_id as "Transaction_ID"
  , transaction_datetime as "Transaction_DateTime"
  , group_id as "Group_ID"
  , round(sum(sku_cost), 2) as "Group_Cost"
  , round(sum(sku_summ), 2) as "Group_Summ"
  , round(sum(sku_summ_paid), 2) as "Group_Summ_Paid"
from main
group by
    customer_id
  , transaction_id
  , transaction_datetime
  , group_id;

/* Periods View */

drop view if exists periods cascade;
create view periods as
select
    customer_id as "Customer_ID"
  , group_id as "Group_ID"
  , min(transaction_datetime) as "First_Group_Purchase_Date"
  , max(transaction_datetime) as "Last_Group_Purchase_Date"
  , count(*) as "Group_Purchase"
  , ((max(transaction_datetime)::date - min(transaction_datetime)::date) + 1)/ count(*) as "Group_Frequency"
  , round(coalesce(min(case when sku_discount = 0 then null
			  else sku_discount/sku_summ end), 0), 2) as "Group_Min_Discount"
from main
group by
    customer_id
  , group_id;
  