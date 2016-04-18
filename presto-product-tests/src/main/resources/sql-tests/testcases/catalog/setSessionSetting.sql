-- database: presto; groups: catalog,session_variables;
--!
show session
--!
-- delimiter: |; trimValues: true; ignoreExcessRows: true; ignoreOrder: true
distributed_join                        | true       | true        | boolean | Use a distributed join instead of a broadcast join |
-- database: presto; groups: catalog,session_variables;
--!
set session distributed_join = false;
show session
--!
-- delimiter: |; trimValues: true; ignoreExcessRows: true; ignoreOrder: true
distributed_join                        | false       | true        | boolean | Use a distributed join instead of a broadcast join |
-- database: presto; groups: catalog,session_variables;
--!
set session distributed_join = true;
show session
--!
-- delimiter: |; trimValues: true; ignoreExcessRows: true;  ignoreOrder: true
distributed_join                        | true       | true        | boolean | Use a distributed join instead of a broadcast join |

