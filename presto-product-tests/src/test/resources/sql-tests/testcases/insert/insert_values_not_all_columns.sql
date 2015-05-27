-- database: presto; groups: insert, quarantine; mutable_tables: datatype|created;
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.datatype} values(1, 2.1, 'abc');
select * from ${mutableTables.datatype}
--!
1|2.1|abc|null|null|null|
