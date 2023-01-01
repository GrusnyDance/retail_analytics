

select
	p."Customer_ID" as customer_id
  , p."Group_ID" as group_id
  , p."Last_Group_Purchase_Date" as last_date
  , round((p."Group_Purchase"::numeric / count(*)), 2) as group_affinity_index
  , round(((select * from date_of_analysis_formation order by desc 1 limit 1)::date 
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
  , p."Group_Frequency"
