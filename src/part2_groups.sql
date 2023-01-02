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



  
