-- database: presto; groups: insert, quarantine; mutable_tables: datatype|created
-- delimiter: |; ignoreOrder: true;
--!
insert into ${mutableTables.datatype} values(1.5,2,3,'2014-01-01', '2015-01-01 03:15:16', 6);
select * from ${mutableTables.datatype}
--!
1|2|3|2014-01-01|2015-01-01 03:15:16|t|

