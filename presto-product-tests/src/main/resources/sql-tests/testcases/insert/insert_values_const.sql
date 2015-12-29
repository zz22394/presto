-- database: presto; groups: insert; mutable_tables: datatype_no_decimal|created;
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.hive.datatype_no_decimal} values(1, 2.1, 'abc', cast('2014-01-01' as date), cast('2015-01-01 03:15:16 UTC' as timestamp), FALSE);
select * from ${mutableTables.hive.datatype_no_decimal}
--!
1|2.1|abc|2014-01-01|2015-01-01 03:15:16|f|
