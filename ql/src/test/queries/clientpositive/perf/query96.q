set hive.mapred.mode=nonstrict;
-- start query 1 in stream 0 using template query96.tpl and seed 1819994127
explain
select  count(*) as c
from store_sales
    ,household_demographics 
    ,time_dim, store
where ss_sold_time_sk = time_dim.t_time_sk   
    and ss_hdemo_sk = household_demographics.hd_demo_sk 
    and ss_store_sk = s_store_sk
    and time_dim.t_hour = 8
    and time_dim.t_minute >= 30
    and household_demographics.hd_dep_count = 5
    and store.s_store_name = 'ese'
order by c
limit 100;

-- end query 1 in stream 0 using template query96.tpl
-- this query has been modified so that count(*) has an alias and the query is ordered on this alias; functionally, the
-- query is exactly the same. This is necessary because CDH Hive does not support ordering by unselected columns;
-- upstream Hive has this feature (HIVE-15160), but it is CBO specific
