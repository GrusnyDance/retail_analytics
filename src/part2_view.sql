
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
  
  
  

