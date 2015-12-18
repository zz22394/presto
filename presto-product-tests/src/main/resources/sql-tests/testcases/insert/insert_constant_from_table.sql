-- database: presto; groups: insert; mutable_tables: datatype_no_decimal|created; tables: datatype_no_decimal
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.hive.datatype_no_decimal} select 1, 2.2, 'abc', cast('2014-01-01' as date), cast('2015-01-01 03:15:16 UTC' as timestamp), false from datatype_no_decimal;
select * from ${mutableTables.hive.datatype_no_decimal}
--!
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|

