
drop function if exists frequency_of_visits(timestamp, timestamp, bigint, numeric, numeric, numeric);
create function frequency_of_visits(first_date timestamp default '2018-01-20 08:24:58',
									 last_date timestamp default '2022-08-20 07:31:15',
									 add_trans bigint default 10,
								     max_churn_index numeric default 10,
								     max_disc_share numeric default 85,
								     allow_margin_share numeric default 10)
returns table ("Customer_ID" bigint, 
			    "Start_Date" timestamp, 
			    "End_Date" timestamp,
			    "Required_Transactions_Count" numeric,
			    "Group_Name" varchar,
			    "Offer_Discount_Depth" numeric)
as $$
declare
    in_interv int := (last_date::date - first_date::date);
begin
return query
with group_name as (
	select distinct
		gv."Customer_ID" as customer_id
	  , first_value(gsku.group_name) over (partition by gv."Customer_ID" order by gv."Group_Affinity_Index" desc) as gr_name
	  , first_value(gv."Group_Margin") over (partition by gv."Customer_ID" order by gv."Group_Affinity_Index" desc) as gr_margin
	from groups_view as gv
	join groups_sku as gsku
	  on gsku.group_id = gv."Group_ID"
	where "Group_Churn_Rate" <= 10
	  and "Group_Discount_Share" <  85::numeric/100
	  and "Group_Margin" * 10::numeric/100 > "Group_Minimum_Discount"+0.05 )
select
    cs.customer_id
  , first_date
  , last_date
  , case when cs.customer_frequency = 0 then add_trans
      else (round(in_interv/cs.customer_frequency, 0) + add_trans) end
  , gn.gr_name
  , round(allow_margin_share/100 * gn.gr_margin , 2)
from customers as cs
  join group_name as gn
    on cs.customer_id = gn.customer_id;
end;
$$ language plpgsql;

select * from frequency_of_visits();
