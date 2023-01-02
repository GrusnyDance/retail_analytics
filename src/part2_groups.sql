drop view if exists affinity_index;
create view affinity_index as
	select
		 p."Customer_ID" as customer_id
	   , p."Group_ID" as group_id
	   , round((p."Group_Purchase"::numeric / count(*)), 2) as group_affinity_index
	from periods as p
	join purchase_history as ph
	  on ph."Customer_ID" = p."Customer_ID"
	  and ph."Transaction_DateTime" >= p."First_Group_Purchase_Date"
	  and ph."Transaction_DateTime" <= p."Last_Group_Purchase_Date"
	group by
		p."Customer_ID"
	  , p."Group_ID"
	  , p."Group_Purchase";


drop view if exists churn_rate;
create view churn_rate as
	select
		 p."Customer_ID" as customer_id
	   , p."Group_ID" as group_id
	   , case when p."Group_Frequency" = 0 then 0
			else round(((select * from date_of_analysis_formation order by 1 desc limit 1)::date 
				 - p."Last_Group_Purchase_Date"::date)::numeric / p."Group_Frequency" , 2) end as churn_rate
	from periods as p
	join purchase_history as ph
	  on ph."Customer_ID" = p."Customer_ID"
	group by
		p."Customer_ID"
	  , p."Group_ID"
	  , p."Last_Group_Purchase_Date"
	  , p."Group_Frequency";


drop view if exists stability_index;
create view stability_index as
with stability_temp as (
	select
		ph."Customer_ID" as customer_id
	  , ph."Group_ID" as group_id
	  , ph."Transaction_DateTime" as tr_date
	  , coalesce((ph."Transaction_DateTime"::date - lag(ph."Transaction_DateTime") 
			over (partition by ph."Customer_ID", ph."Group_ID" order by ph."Transaction_DateTime")::date), 0) as intervals
	  , p."Group_Frequency" as gr_frequency
	from purchase_history as ph
	join periods as p
	  on p."Customer_ID" = ph."Customer_ID"
	  and p."Group_ID" = ph."Group_ID")
select
    customer_id
  , group_id
  , round(avg(case when gr_frequency = 0 then 0
				else (case when (gr_frequency > intervals) then gr_frequency - intervals
				  		else intervals - gr_frequency end)::numeric / gr_frequency end), 2) as stability_index
from stability_temp as st
group by
    customer_id
  , group_id


drop view if exists discount_share_min;
create view discount_share_min as
with discount_transaction as (
	select
		m.customer_id
	  , m.group_id
	  , count(distinct transaction_id) as qty_dis_tr
	from main as m
	where m.sku_discount > 0
	group by
		m.customer_id
	  , m.group_id)  
select
    p."Customer_ID" as customer_id
  , p."Group_ID" as group_id
  , round(coalesce (dt.qty_dis_tr, 0)::numeric / p."Group_Purchase", 2) as group_discount_share
  , p."Group_Min_Discount" as group_min_discount
from discount_transaction as dt
right join periods as p
  on p."Customer_ID" = dt.customer_id
  and p."Group_ID" = dt.group_id;


drop view if exists group_average_discount;
create view group_average_discount as
	select
		"Customer_ID" as customer_id
	  , "Group_ID" as group_id
	  , round (sum("Group_Summ_Paid") / sum("Group_Summ") , 2) as group_average_discount
	from purchase_history
	group by
		"Customer_ID"
	  , "Group_ID";


drop function if exists group_margin(int, int);
create function group_margin(mode_margin int default 3, in_value int default 100) 
returns table (customer_id bigint, group_id bigint, group_margin numeric)
as $$
begin
if mode_margin = 1 then 
  return query
	select
		"Customer_ID" as customer_id
	  , "Group_ID" as group_id
	  , sum("Group_Summ_Paid") - sum("Group_Cost") as group_margin
	from purchase_history
	where "Transaction_DateTime"::date >= 
		((select * from date_of_analysis_formation order by 1 desc limit 1)::date - in_value)
	group by
		"Customer_ID"
	  , "Group_ID";
elsif mode_margin = 2 then
   return query
    select
	    lph.customer_id
	  , lph.group_id
	  , sum(lph.margin) as group_margin
	from (select
		      "Customer_ID" as customer_id
			, "Group_ID" as group_id
			, "Group_Summ_Paid" - "Group_Cost" as margin
		  from purchase_history
		  order by "Transaction_DateTime" desc
		  limit in_value) as lph
	group by
	    lph.customer_id
	  , lph.group_id;
else
   return query
	select
		"Customer_ID" as customer_id
	  , "Group_ID" as group_id
	  , sum("Group_Summ_Paid") - sum("Group_Cost") as group_margin
	from purchase_history
	group by
		"Customer_ID"
	  , "Group_ID";
end if;
end;
$$ language plpgsql;


drop view if exists groups_view;
create view groups_view as
	select
		gm.customer_id as "Customer_ID"
	  , gm.group_id as "Group_ID"
	  , ai.group_affinity_index as "Group_Affinity_Index"
	  , cr.churn_rate as "Group_Churn_Rate"
	  , si.stability_index as "Group_Stability_Index"
	  , gm.group_margin as "Group_Margin"
	  , dsm.group_discount_share as "Group_Discount_Share"
	  , dsm.group_min_discount as "Group_Minimum_Discount"
	  , gad.group_average_discount as "Group_Average_Discount"
	from group_margin() as gm  /* group_margin(mode, in_value) mode 1 for period, mode 2 for qty. 
	                                                           in_value - qty of days or transaction */
	  join affinity_index as ai
		on ai.customer_id = gm.customer_id
		and ai.group_id = gm.group_id
	  join churn_rate as cr
		on cr.customer_id = gm.customer_id
		and cr.group_id = gm.group_id
	  join stability_index as si
		on si.customer_id = gm.customer_id
		and si.group_id = gm.group_id
	  join discount_share_min as dsm
		on dsm.customer_id = gm.customer_id
		and dsm.group_id = gm.group_id
	  join group_average_discount as gad
		on gad.customer_id = gm.customer_id
		and gad.group_id = gm.group_id;
