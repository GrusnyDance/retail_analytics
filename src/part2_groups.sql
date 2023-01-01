
select
     p."Customer_ID"
   , p."Group_ID"
   , round((p."Group_Purchase"::numeric / count(*)), 2) as "Group_Affinity_Index"
from periods as p
join purchase_history as ph
  on ph."Customer_ID" = p."Customer_ID"
  and ph."Transaction_DateTime" >= p."First_Group_Purchase_Date"
  and ph."Transaction_DateTime" <= p."Last_Group_Purchase_Date"
group by
	p."Customer_ID"
  , p."Group_ID"
  , p."Group_Purchase"


