-- database: presto; groups: insert; mutable_tables: datatype_no_decimal|created;
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.hive.datatype_no_decimal} values(5 * 10, 4.1 + 5, 'abc', cast('2014-01-01' as date), cast('2015-01-01 03:15:16 UTC' as timestamp), TRUE);
select * from ${mutableTables.hive.datatype_no_decimal}
--!
50|9.1|abc|2014-01-01|2015-01-01 03:15:16|true|
