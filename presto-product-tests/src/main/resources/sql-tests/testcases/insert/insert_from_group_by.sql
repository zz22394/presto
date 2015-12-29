-- database: presto; groups: insert; mutable_tables: datatype_no_decimal|created; tables: datatype_no_decimal
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.hive.datatype_no_decimal} select count(*), 1.1, 'a', cast('2016-01-01' as date), cast('2015-01-01 03:15:16 UTC' as timestamp), FALSE from datatype_no_decimal group by c_bigint;
select * from ${mutableTables.hive.datatype_no_decimal}
--!
1|1.1|a|2016-01-01|2015-01-01 03:15:16|f|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|f|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|f|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|f|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|f|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|f|
4|1.1|a|2016-01-01|2015-01-01 03:15:16|f|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|f|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|f|
4|1.1|a|2016-01-01|2015-01-01 03:15:16|f|
