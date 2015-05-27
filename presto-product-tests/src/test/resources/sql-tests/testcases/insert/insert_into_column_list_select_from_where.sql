-- database: presto; groups: insert, quarantine; mutable_tables: datatype|created; tables: datatype
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.datatype} (c_bigint, c_double, c_varchar) select c_bigint, c_double, c_varchar from datatype where c_varchar > 's';
select * from ${mutableTables.datatype}
--!
25|55.52|test|null|null|null|
100|12.25|testing|null|null|null|
5252|12.25|sample|null|null|null|
100|12.8788|string1|null|null|null|
5748|67.87|sample|null|null|null|
5748|67.87|sample|null|null|null|
5748|67.87|sample|null|null|null|
5000|67.87|testing|null|null|null|
