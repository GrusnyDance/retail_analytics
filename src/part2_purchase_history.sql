create or replace view purchase_history as
with main as (
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
		and st.sku_id = sku.sku_id)
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