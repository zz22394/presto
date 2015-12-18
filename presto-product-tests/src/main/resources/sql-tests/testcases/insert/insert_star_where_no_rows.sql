-- database: presto; groups: insert; mutable_tables: datatype_no_decimal|created; tables: datatype_no_decimal
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.hive.datatype_no_decimal} select * from datatype_no_decimal where c_bigint < 0;
select * from ${mutableTables.hive.datatype_no_decimal}
--!
