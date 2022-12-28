create view purchase_history as
select
    pd.customer_id as "Customer_ID"
  , tr.transaction_id as "Transaction_ID"
  , tr.transaction_datetime as "Transaction_DateTime"
  , sku.group_id as "Group_ID"
  , round(ch.sku_amount * st.sku_purchase_price, 2) as "Group_Cost"
  , round(ch.sku_summ, 2) as "Group_Summ"
  , round(ch.sku_summ_paid, 2) as "Group_Summ_Paid"
from transactions as tr
  join cards as cd
    on cd.customer_card_id = tr.customer_card_id
  join personal_data as pd
    on cd.customer_id = pd.customer_id
  join checks as ch
    on ch.transaction_id = tr.transaction_id
  join sku
    on sku.sku_id = ch.sku_id
  join stores as st
    on st.transaction_store_id = tr.transaction_store_id
	and st.sku_id = sku.sku_id;