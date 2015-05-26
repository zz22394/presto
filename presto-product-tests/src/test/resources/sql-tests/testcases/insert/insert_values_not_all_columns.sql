-- database: presto; groups: insert; mutable_tables: datatype|created; 
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.datatype} values(1, 2, 'abc');
select * from ${mutableTables.datatype};
--!
1|2|abc|null|null|null|
