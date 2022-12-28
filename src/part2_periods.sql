create or replace view periods as
select
    ph."Customer_ID" 
  , ph."Group_ID"
  , min("Transaction_DateTime") as "First_Group_Purchase_Date"
  , max("Transaction_DateTime") as "Last_Group_Purchase_Date"
  , count(*) as "Group_Purchase"
  , ((max("Transaction_DateTime")::date - min("Transaction_DateTime")::date) + 1)/ count(*) as "Group_Frequency"
  , round(min(ch.sku_discount/ch.sku_summ), 2) as "Group_Min_Discount"
from purchase_history as ph
  join checks as ch
    on ph."Transaction_ID" = ch.transaction_id
group by
    ph."Customer_ID"
  , ph."Group_ID";
  