-- database: presto; groups: mysql_connector;
-- queryType: SELECT;
--!
show tables from mysql.test
--!
-- delimiter: |; trimValues: true; ignoreOrder: true;
datatype_jdbc|
workers_jdbc|