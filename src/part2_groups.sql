
with affinity_churn as(
		select
			 p."Customer_ID" as customer_id
		   , p."Group_ID" as group_id
		   , p."Last_Group_Purchase_Date" as last_date
		   , p."Group_Frequency" as gr_frequency
		   , round((p."Group_Purchase"::numeric / count(*)), 2) as group_affinity_index
		   , round(((select * from date_of_analysis_formation order by 1 limit 1)::date 
					 - p."Last_Group_Purchase_Date"::date)::numeric / p."Group_Frequency" , 2) as churn_rate
		from periods as p
		join purchase_history as ph
		  on ph."Customer_ID" = p."Customer_ID"
		  and ph."Transaction_DateTime" >= p."First_Group_Purchase_Date"
		  and ph."Transaction_DateTime" <= p."Last_Group_Purchase_Date"
		group by
			p."Customer_ID"
		  , p."Group_ID"
		  , p."Group_Purchase"
		  , p."Last_Group_Purchase_Date"
		  , p."Group_Frequency"),
	stability_temp as (
		select
			ac.customer_id
		  , ac.group_id
		  , ac.group_affinity_index
		  , ac.churn_rate
		  , ph."Transaction_DateTime" as tr_date
		  , ac.gr_frequency
		  , coalesce((ph."Transaction_DateTime"::date - lag(ph."Transaction_DateTime") 
					  over (partition by customer_id, group_id order by ph."Transaction_DateTime")::date), 0) as intervals
		from affinity_churn as ac
		join purchase_history as ph
		  on ph."Customer_ID" = ac.customer_id
		  and ph."Group_ID" = ac.group_id),
	stability_index as (
		select
			customer_id
		  , group_id
		  , group_affinity_index
		  , churn_rate
		  , (case when (gr_frequency > intervals) then gr_frequency - intervals
				else intervals - gr_frequency end)::numeric / gr_frequency as diff_from_avr
		from stability_temp)
select
    customer_id
  , group_id
  , group_affinity_index
  , churn_rate
  , round(avg(diff_from_avr), 2) as stability_index
from stability_index
group by
    customer_id
  , group_id
  , group_affinity_index
  , churn_rate
