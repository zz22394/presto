-- database: presto; groups: insert; mutable_tables: datatype|created; tables: datatype
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.datatype} select 1,2,'abc \n def' from datatype;
select * from ${mutableTables.datatype};
--!
1|2|abc \n def|
1|2|abc \n def|
1|2|abc \n def|
1|2|abc \n def|
1|2|abc \n def|
1|2|abc \n def|
1|2|abc \n def|
1|2|abc \n def|
1|2|abc \n def|
1|2|abc \n def|
1|2|abc \n def|
1|2|abc \n def|
1|2|abc \n def|
1|2|abc \n def|
1|2|abc \n def|
1|2|abc \n def|
