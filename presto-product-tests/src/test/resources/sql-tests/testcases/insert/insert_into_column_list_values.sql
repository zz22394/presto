-- database: presto; groups: insert, quarantine; mutable_tables: datatype|created
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.datatype} (c_bigint, c_double, c_varchar) values(1,2,'a');
select * from ${mutableTables.datatype}
--!
1|2|a|null|null|null|
