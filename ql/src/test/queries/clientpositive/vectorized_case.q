set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.vectorized.execution.enabled = true;

explain vectorization detail
select 
  csmallint,
  case 
    when csmallint = 418 then "a"
    when csmallint = 12205 then "b"
    else "c"
  end,
  case csmallint
    when 418 then "a"
    when 12205 then "b"
    else "c"
  end
from alltypesorc
where csmallint = 418
or csmallint = 12205
or csmallint = 10583
;
select 
  csmallint,
  case 
    when csmallint = 418 then "a"
    when csmallint = 12205 then "b"
    else "c"
  end,
  case csmallint
    when 418 then "a"
    when 12205 then "b"
    else "c"
  end
from alltypesorc
where csmallint = 418
or csmallint = 12205
or csmallint = 10583
;
explain vectorization detail
select 
  csmallint,
  case 
    when csmallint = 418 then "a"
    when csmallint = 12205 then "b"
    else null
  end,
  case csmallint
    when 418 then "a"
    when 12205 then null
    else "c"
  end
from alltypesorc
where csmallint = 418
or csmallint = 12205
or csmallint = 10583
;

select count(*), sum(a.ceven)
from (
select
  case when cint % 2 = 0 then 1 else 0 end as ceven
from alltypesorc) a;

select count(*)
from (
select
  (case when cint % 2 = 0 then 1 else 0 end) as ceven
from alltypesorc
where (case when cint % 2 = 0 then 1 else 0 end) = 0) a;

select count(*)
from (
select
  (case when cint % 2 = 0 then 1 else 0 end) as ceven
from alltypesorc
where (case when cint % 2 = 0 then 1 else 0 end) = 0 AND cint is NOT NULL) a;

select count(*)
from (
select
  (case when cint % 2 = 0 then 1 else 0 end) as ceven
from alltypesorc
where (case when cint % 2 = 0 then 1 else 0 end) = 1) a;

select count(*)
from (
select
  (case when cint % 2 = 0 then 1 else 0 end) as ceven
from alltypesorc
where (case when cint % 2 = 0 then 1 else 0 end) = 1 AND cint is NOT NULL) a;

select count(*)
from (
select
  (case when cint % 2 = 0 then 1 else 0 end) as ceven
from alltypesorc
where cint is null) a;


select count(*), sum(a.ceven)
from (
select
  case when cint % 2 = 0 then cint else 0 end as ceven
from alltypesorc) a;

select count(*)
from (
select
  (case when cint % 2 = 0 then cint else 0 end) as ceven
from alltypesorc
where (case when cint % 2 = 0 then cint else 0 end) = 0) a;

select count(*)
from (
select
  (case when cint % 2 = 0 then cint else 0 end) as ceven
from alltypesorc
where (case when cint % 2 = 0 then cint else 0 end) = 0 AND cint is NOT NULL) a;

select count(*)
from (
select
  (case when cint % 2 = 0 then cint else 0 end) as ceven
from alltypesorc
where (case when cint % 2 = 0 then cint else 0 end) = cint) a;

select count(*)
from (
select
  (case when cint % 2 = 0 then cint else 0 end) as ceven
from alltypesorc
where (case when cint % 2 = 0 then cint else 0 end) = cint AND cint is NOT NULL) a;

select count(*)
from (
select
  (case when cint % 2 = 0 then cint else 0 end) as ceven
from alltypesorc
where cint is null) a;
