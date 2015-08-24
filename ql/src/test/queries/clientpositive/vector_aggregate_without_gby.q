create table testvec(id int, dt int, greg_dt string) stored as orc;
load data local inpath '../../data/files/testvec.orc' overwrite into table testvec;

set hive.vectorized.execution.enabled=true;
set hive.map.aggr=true;
explain select max(dt), max(greg_dt) from testvec where id=5;
select max(dt), max(greg_dt) from testvec where id=5;