-- database: presto; groups: insert; mutable_tables: datatype|created
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.datatype} select 1, 2, 'abc', '2014-01-01', '2015-01-01 03:15:16', FALSE;
select * from ${mutableTables.datatype};
--!
1|2|abc|2014-01-01|2015-01-01 03:15:16|f|
