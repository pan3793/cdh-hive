set hive.mapred.mode=nonstrict;
-- start query 1 in stream 0 using template query84.tpl and seed 1819994127
explain
select  c_customer_id as customer_id
       , coalesce(c_last_name,'') || ', ' || coalesce(c_first_name,'') as customername
 from customer
     ,customer_address
     ,customer_demographics
     ,household_demographics
     ,income_band
     ,store_returns
 where ca_city	        =  'Hopewell'
   and c_current_addr_sk = ca_address_sk
   and ib_lower_bound   >=  32287
   and ib_upper_bound   <=  32287 + 50000
   and ib_income_band_sk = hd_income_band_sk
   and cd_demo_sk = c_current_cdemo_sk
   and hd_demo_sk = c_current_hdemo_sk
   and sr_cdemo_sk = cd_demo_sk
 order by customer_id
 limit 100;

-- end query 1 in stream 0 using template query84.tpl
-- this query has been modified so that query is ordered by customer_id instead of c_customer_id; functionally, this is
-- ordering by the same column. The change is necessary because Hive does not support ordering by unselected columns;
-- upstream Hive has this feature (HIVE-15160), but it is CBO specific
