drop view if exists sku_share_group;
create view	sku_share_group as					
select
    m1.group_id
  , m1.sku_id
  , m1.sku_qty::numeric / m2.gr_qty as sku_share
from (	select group_id, sku_id, count(distinct transaction_id) as sku_qty
		from main
		group by group_id, sku_id) as m1
join (  select group_id, count(distinct transaction_id) as gr_qty
	    from main group by group_id) as m2
  on m1.group_id = m2.group_id;


drop function if exists cross_selling(int, numeric, numeric, numeric, numeric);
create function cross_selling(group_qty int default 5,
							   max_churn_index numeric default 25,
							   max_stability_index numeric default 2.5,
							   max_sku_share numeric default 70,
							   allow_margin_share numeric default 60)
returns table ("Customer_ID" bigint, 
			    "SKU_Name" varchar,
			    "Offer_Discount_Depth" numeric)
as $$
begin
return query
with step_one as (
		select
			t1."Customer_ID" as customer_id
		  , t1."Group_ID" as group_id
		  , cus.customer_primary_store as c_store
		from (select *,
				 row_number() over (partition by gv."Customer_ID" order by gv."Group_Affinity_Index" desc) as i
			  from groups_view as gv 
			  where gv."Group_Churn_Rate" <= max_churn_index
				and gv."Group_Stability_Index" < max_stability_index) as t1
		join customers as cus
		  on cus.customer_id = t1."Customer_ID"
		where i <= group_qty),
	step_two as (
		select 
		    s1.customer_id
	      , s1.group_id
		  , s1.c_store
		  , temp.sku_id
		  , temp.diff_price
		  , temp.sku_retail_price
		from step_one as s1
		join (select
				  st.transaction_store_id
				, st.sku_id
			    , st.sku_retail_price
				, st.sku_retail_price - st.sku_purchase_price as diff_price
				, sku.group_id
				, row_number() over (partition by st.transaction_store_id, sku.group_id 
									 order by (st.sku_retail_price - st.sku_purchase_price) desc) as i
			  from stores as st
			  join sku
				on st.sku_id = sku.sku_id) as temp
		  on temp.transaction_store_id = s1.c_store
		  and temp.group_id = s1.group_id
		where temp.i = 1),
	step_three as (
		select
		    s2.customer_id
		  , s2.group_id
		  , s2.c_store
		  , s2.sku_id
		  , (s2.diff_price * allow_margin_share::numeric / 100) / s2.sku_retail_price as discount
		  , case when round(p."Group_Min_Discount" / 0.05) * 0.05 < p."Group_Min_Discount"
        		then (round(p."Group_Min_Discount" / 0.05) * 0.05 + 0.05)*100
        		else (round(p."Group_Min_Discount" / 0.05) * 0.05)*100 end as min_discount
		from step_two as s2
		join periods as p
		  on s2.customer_id = p."Customer_ID"
		  and s2.group_id = p."Group_ID"
		join sku_share_group as ssg
		  on ssg.sku_id = s2.sku_id
		  and ssg.group_id = s2.group_id
		where sku_share <= max_sku_share::numeric / 100)
select
    customer_id
  , sku.sku_name
  , min_discount
from step_three as s3
join sku
  on sku.sku_id = s3.sku_id
where discount >= min_discount;

end;
$$ language plpgsql;

select * from cross_selling();
