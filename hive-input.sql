show tables
select * from bryce_table
create table test_table as select a, b from bryce_table
select * from test_table
drop table test_table
show tables
