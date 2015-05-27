-- database: presto; groups: insert, quarantine; mutable_tables: datatype|created; tables: datatype
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.datatype} select c_bigint, c_double, c_varchar from datatype where c_double < 20;
select * from ${mutableTables.datatype}
--!
12|12.25|String1|
964|0.245|Again|
100|12.25|testing|
5252|12.25|sample|
100|9.8777|STRING1|
100|12.8788|string1|
