-- database: presto; groups: insert; mutable_tables: datatype_no_decimal|created; tables: datatype_no_decimal
-- delimiter: |; ignoreOrder: true;
--!
insert into ${mutableTables.hive.datatype_no_decimal} select * from datatype_no_decimal order by 1 limit 2;
select * from ${mutableTables.hive.datatype_no_decimal}
--!
12|12.25|String1|1999-01-08|1999-01-08 02:05:06|true|
25|55.52|test|1952-01-05|1989-01-08 04:05:06|false|
