-- database: presto; groups: insert; mutable_tables: datatype_no_decimal|created; tables: datatype_no_decimal
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.hive.datatype_no_decimal} select 1,2.1,'abc \n def', cast(null as date), cast(null as timestamp), cast(null as boolean) from datatype_no_decimal;
select * from ${mutableTables.hive.datatype_no_decimal}
--!
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
