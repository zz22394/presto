-- database: presto; groups: catalog,session_variables; queryType: SELECT;
--!
show session
--!
-- delimiter: |; trimValues: true; ignoreOrder: true; ignoreExcessRows: true
columnar_processing                     | false       | false       | boolean | Use columnar processing |
columnar_processing_dictionary          | false       | false       | boolean | Use columnar processing with optimizations for dictionaries |
dictionary_aggregation                  | false       | false       | boolean | Enable optimization for aggregations on dictionaries |
distributed_index_join                  | false       | false       | boolean | Distribute index joins on join keys instead of executing inline |
distributed_join                        | true        | true        | boolean | Use a distributed join instead of a broadcast join |
execution_policy                        | all-at-once | all-at-once | varchar | Policy used for scheduling query tasks |
hash_partition_count                    | 8           | 8           | bigint  | Number of partitions for distributed joins and aggregations |
initial_splits_per_node                 | 4           | 4           | bigint  | The number of splits each node will run per task, initially |
optimize_hash_generation                | true        | true        | boolean | Compute hash codes for distribution, joins, and aggregations early in query plan |
parse_decimal_literals_as_double        | false       | false       | boolean | Parse decimal literals as DOUBLE instead of DECIMAL |
prefer_streaming_operators              | false       | false       | boolean | Prefer source table layouts that produce streaming operators |
push_table_write_through_union          | true        | true        | boolean | Parallelize writes when using UNION ALL in queries that write data |
query_max_run_time                      | 100.00d     | 100.00d     | varchar | Maximum run time of a query |
re2j_dfa_retries                        | 5           | 5           | bigint  | Set a number of DFA retries before switching to NFA |
re2j_dfa_states_limit                   | 2147483647  | 2147483647  | bigint  | Set a DFA states limit |
redistribute_writes                     | true        | true        | boolean | Force parallel distributed writes |
regex_library                           | JONI        | JONI        | varchar | Select the regex library |
resource_overcommit                     | false       | false       | boolean | Use resources which are not guaranteed to be available to the query |
task_share_index_loading                | false       | false       | boolean | Share index join lookups and caching within a task |
task_writer_count                       | 1           | 1           | bigint  | Default number of local parallel table writer jobs per worker |
hive.force_local_scheduling             | false       | false       | boolean | Only schedule splits on workers colocated with data node |
hive.orc_max_buffer_size                | 8MB         | 8MB         | varchar | ORC: Maximum size of a single read |
hive.orc_max_merge_distance             | 1MB         | 1MB         | varchar | ORC: Maximum size of gap between two reads to merge into a single read |
hive.orc_stream_buffer_size             | 8MB         | 8MB         | varchar | ORC: Size of buffer for streaming reads |

