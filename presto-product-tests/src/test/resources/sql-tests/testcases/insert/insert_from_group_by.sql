-- database: presto; groups: insert; mutable_tables: datatype|created; tables: datatype
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.datatype} select count(*), 1, 'a', '2016-01-01', '2015-01-01 03:15:16', FALSE from datatype group by c_bigint;
select * from ${mutableTables.datatype}
--!
1|1|a|2016-01-01|2015-01-01 03:15:16|f|
1|1|a|2016-01-01|2015-01-01 03:15:16|f|
1|1|a|2016-01-01|2015-01-01 03:15:16|f|
1|1|a|2016-01-01|2015-01-01 03:15:16|f|
1|1|a|2016-01-01|2015-01-01 03:15:16|f|
1|1|a|2016-01-01|2015-01-01 03:15:16|f|
4|1|a|2016-01-01|2015-01-01 03:15:16|f|
1|1|a|2016-01-01|2015-01-01 03:15:16|f|
1|1|a|2016-01-01|2015-01-01 03:15:16|f|
4|1|a|2016-01-01|2015-01-01 03:15:16|f|
