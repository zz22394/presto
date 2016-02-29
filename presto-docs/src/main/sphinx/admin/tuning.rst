=============
Tuning Presto
=============

The default Presto settings should work well for most workloads. The following
information may help you if your cluster is facing a specific performance problem.

.. _tuning-pref-general:

General properties
------------------


``distributed-index-joins-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Enabling this property forces repartitioning of joined tables on their keys. This causes an increase in processing time, but reduces the amount of memory required, and therefore allows much larger joins to be performed. After filtering, the index of the table on the left side of the join must fit in memory on each node. This can also be specified on a per-query basis using the ``distributed_index_join`` session property.


``distributed-joins-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:** Use hash distributed joins instead of broadcast joins. Distributed joins require redistributing both tables using a hash of the join key. This can be slower (sometimes substantially) than broadcast joins but allows much larger joins. Broadcast joins require that the tables on the right side of the join after filtering fit in memory on each node whereas distributed joins only need to fit in distributed memory across all nodes. This can also be specified on a per-query basis using the ``distributed_join`` session property.


``redistribute-writes``
^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:** Force parallel distributed writes. Setting this property will cause the write operator to be distributed between nodes. This allows allows Presto to more efficiently utilize distributed storage across the cluster. It is particularly effective when executing a small number of huge queries. This can also be specified on a per-query basis using the ``redistribute_writes`` session property.


``resources.reserved-system-memory``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``JVM max memory`` * ``0.4``
 * **Description:** Maximum amount of memory available to each Presto node. Reaching this limit will cause the server to drop operations. Higher value may increase Presto's stability, but may cause problems if physical server is used for other purposes. If too much memory is allocated to Presto, the operating system may terminate the process.


``sink.max-buffer-size``
^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``32 MB``
 * **Description:** Buffer size for IO writes while collecting pipeline results. Increasing this value may improve the speed of IO operations, but will take memory away from other functions. Buffered data will be lost if the node crashes, so using a large value is not recommended when the environment is unstable.


.. _tuning-pref-exchange:

Exchange properties
-------------------

The Exchange service is responsible for transferring data between Presto nodes. Adjusting these properties may help to resolve inter-node communication issues or improve network utilization.

``exchange.client-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``25``
 * **Description:** Number of threads that the exchange server can spawn to handle clients. Higher value will increase concurrency but excessively high values may cause a drop in performance due to context switches and additional memory usage.


``exchange.concurrent-request-multiplier``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``3``
 * **Description:** Multiplier determining how many clients of the exchange server may be spawned relative to available buffer memory. The number of possible clients is determined by heuristic as the number of clients that can fit into available buffer space based on average buffer usage per request times this multiplier. For example with the ``exchange.max-buffer-size`` of ``32 MB`` and ``20 MB`` already used, and average bytes per request being ``2MB`` up to ``exchange.concurrent-request-multipier`` * ((``32MB`` - ``20MB``) / ``2MB``) = ``exchange.concurrent-request-multiplier`` * ``6`` may be spawned. Tuning this value adjusts the heuristic, which may increase concurrency and improve network utilization.


``exchange.max-buffer-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``32 MB``
 * **Description:** Size of memory block reserved for the client buffer in exchange server. Lower value may increase processing time under heavy load. Increasing this value may improve network utilization, but will reduce the amount of memory available for other activities.


``exchange.max-response-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size, at least ``1 MB``)
 * **Default value:** ``16 MB``
 * **Description:** Max size of messages sent through the exchange server. The size of message headers is included in this value, so the amount of data sent per message will be a little lower. Increasing this value may improve network utilization if the network is stable. In an unstable network environment, making this value smaller may improve stability.


.. _tuning-pref-node:

Node scheduler properties
-------------------------

``node-scheduler.max-pending-splits-per-node-per-stage``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``10``
 * **Description:** Must be smaller than ``node-scheduler.max-splits-per-node``. This property describes how many splits can be queued to each worker node. Having this value higher will allow more jobs to be queued but will cause resources to be used for that. Using a higher value is recommended if queries are submitted in large batches, (eg. running a large group of reports periodically). Increasing this value may help to avoid query drops and decrease the risk of short query starvation.


``node-scheduler.max-splits-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``100``
 * **Description:** This property limits the number of splits that can be scheduled for each node. Increasing this value will allow the cluster to process bigger queries and/or queries with significant skew. Excessively high values may result in poor performance due to context switching and higher memory reservation for cluster metadata.


``node-scheduler.min-candidates``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``10``
 * **Description:** The minimal number of node candidates proposed by scheduler to do each job. Setting this affects global parallelism. For systems with lots of nodes and a small number of huge queries, a higher value may improve performance. For systems with fewer nodes or a large number of small queries, a lower lower value may improve performance. This setting is connected with node-scheduler.network-topology - while using flat it is important to align this value with the backend data distribution.


``node-scheduler.multiple-tasks-per-node-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Allow nodes to be selected multiple times by the node scheduler in a single stage. With this property set to ``false`` the ``node-scheduler.min-candidates`` is capped at number of nodes in system. Having this set to ``true`` may allow better scheduling and concurrency, which would reduce the number of outliers and speed up computations. It may also improve reliability in unstable network conditions. The drawbacks are that some optimization may work less efficiently on smaller partitions. Also slight hardware efficiency drop is expected in heavy loaded system.

.. _node-scheduler-network-topology:

``node-scheduler.network-topology``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (``legacy`` or ``flat``)
 * **Default value:** ``legacy``
 * **Description:** Sets the network topology to use when scheduling splits. ``legacy`` will ignore the topology when scheduling splits. ``flat`` will try to schedule splits on the host where the data is located by reserving 50% of the work queue for local splits. It is recommended to use ``flat`` for clusters where distributed storage runs on the same nodes as Presto workers.


.. _tuning-pref-optimizer:

Optimizer properties
--------------------

``optimizer.processing-optimization``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (``disabled``, ``columnar`` or ``columnar_dictionary``)
 * **Default value:** ``disabled``
 * **Description:** Setting this property changes how filtering operators are processed. Setting it to ``columnar`` allows Presto to use columnar processing instead of row by row. Setting ``columnar_dictionary`` adds additional dictionary to simplify columnar scan. Setting this to a value other than ``disabled`` may improve performance for data containing large rows often filtered by a simple key. This can also be specified on a per-query basis using the ``processing_optimization`` session property.

``optimizer.dictionary-aggregation``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Enables optimization for aggregations on dictionaries. This can also be specified on a per-query basis using the ``dictionary_aggregation`` session property.


``optimizer.optimize-hash-generation``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:** Compute hash codes for distribution, joins, and aggregations early in query plan. While this will increase the preprocessing time, it may allow the optimizer to drop some computations later in query processing. In most cases it will decrease overall query processing time. This can also be specified on a per-query basis using the ``optimize_hash_generation`` session property.


``optimizer.optimize-metadata-queries``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Setting this property to ``true`` enables optimization of some aggregations by using values that are kept in metadata. This allows Presto to execute some simple queries in ``O(1)`` time. Currently this optimization applies to ``max``, ``min`` and ``approx_distinct`` of partition keys. Using this may speed some queries significantly, though it may have a negative effect when used with very small data sets.


``optimizer.optimize-single-distinct``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:** Enables single distinct optimization. This optimization will try to replace multiple DISTINCT clauses with a single GROUP BY clause. Enabling this optimization will speed up some specific SELECT queries, but analyzing all queries to check if they qualify for this optimization may be a slight overhead.


``optimizer.push-table-write-through-union``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:** Parallelize writes when using UNION ALL in queries that write data. This improves the speed of writing output tables in UNION ALL queries because these writes do not require additional synchronization when collecting results. Enabling this optimization can improve UNION ALL speed when write speed is not yet saturated. However it may slow down queries in an already heavy loaded system. This can also be specified on a per-query basis using the ``push_table_write_through_union`` session property.


.. _tuning-pref-query:

Query execution properties
--------------------------


``query.execution-policy``
^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (``all-at-once`` or ``phased``)
 * **Default value:** ``all-at-once``
 * **Description:** Setting this value to ``phased`` will allow the query scheduler to split a single query execution between different time slots. This will allow Presto to switch context more often and possibly stage the partially executed query in order to increase robustness. Average time to execute a query may slightly increase after setting this to ``phased``, but query execution time will be more consistent. This can also be specified on a per-query basis using the ``execution_policy`` session property.


``query.initial-hash-partitions``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``100``
 * **Description:** This value is used to determine how many nodes may share the same query when fixed partitioning is chosen by presto. Manipulating this value will affect the distribution of work between nodes. A value lower then the number of Presto nodes may lower the utilization of the cluster in a low traffic environment. An excessively high value will cause multiple partitions of the same query to be assigned to a single node, or Presto may ignore the setting if ``node-scheduler.multiple-tasks-per-node-enabled`` is set to false - the value is internally capped at the number of available worker nodes in such scenario. This can also be specified on a per-query basis using the ``hash_partition_count`` session property.


``query.low-memory-killer.delay``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration, at least ``5s``)
 * **Default value:** ``5 m``
 * **Description:** Delay between cluster running low on memory and invoking killer. When this value is low, there will be instant reaction for running out of memory on cluster. This may cause more queries to fail fast but it will be less often that query will fail in unexpected way.


``query.low-memory-killer.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** This property controls if there should be killer of query triggered when cluster is running out of memory. The strategy of the killer is to drop largest queries first so enabling this option may cause problem with executing large queries in highly loaded cluster but should increase stability of smaller queries.


``query.manager-executor-pool-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``5``
 * **Description:** Size of thread pool used for garbage collecting after queries. Threads from this pool are used to free resources from canceled queries, enforcing memory limits, queries timeouts etc. Higher number of threads will allow to manage memory more efficiently, so it may be increased to avoid out of memory exceptions in some scenarios. On the other hand higher value here may increase CPU usage for garbage collecting and use additional constant memory even if there is nothing to do for all of the threads.


``query.min-expire-age``
^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration)
 * **Default value:** ``15 m``
 * **Description:** This property describes time after which the query metadata may be removed from server. If value is low, it's possible that client will not be able to receive information about query completion. The value describes minimum time that must pass to remove query (after it's considered completed) but if there is space available in history queue the query data will be kept longer. The size of history queue is defined by ``query.max-history`` property (``100`` by default).


``query.max-concurrent-queries``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``1000``
 * **Description:** **Deprecated** Describes how many queries be processed simultaneously in single cluster node. It shouldn't be used in new configuration, the ``query.queue-config-file`` can be used instead.


.. _query-max-memory:

``query.max-memory``
^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``20 GB``
 * **Description:** Serves as default value for ``query_max_memory`` session property. This property also describes strict limit of total memory allocated around the cluster that may be used to process single query. The query is dropped if the limit is reached unless session want to prevent that by setting session property ``resource_overcommit``. The session may also want to decrease system pressure, so it's possible to decrease query memory limit for session by setting ``query_max_memory`` to smaller value. Setting ``query_max_memory`` to higher value then ``query.max-memory`` will not have any effect. This property may be used to ensure that single query cannot use all resources in cluster. The value should be set to be higher than what typical expected query in system will need - that way system will be resistant to SQL bugs that would cause large unwanted computation. Also if rare queries will require more memory, then the ``resource_overcommit`` session property may be used to break the limit. It is important to set this value to higher then default when presto runs complex queries on large datasets.


``query.max-memory-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``JVM max memory`` * ``0.1``
 * **Description:** The purpose of that is same as of :ref:`query.max-memory<query-max-memory>` but the memory is not counted cluster-wise but node-wise instead.


``query.max-queued-queries``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``5000``
 * **Description:** **Deprecated** Describes how many queries may wait in worker queue. If the limit is reached master server will consider worker blocked and will not push more tasks to him. Setting this value high may allow to order a lot of queries at once with the cost of additional memory needed to keep informations about tasks to process. Lowering this value will decrease system capacity but will allow to utilize memore for real processing of date instead of queuing. It shouldn't be used in new configuration, the ``query.queue-config-file`` can be used instead.


``query.max-run-time``
^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration)
 * **Default value:** ``100 d``
 * **Description:** Used as default for session property ``query_max_run_time``. If the presto works in environment where there are mostly very long queries (over 100 days) than it may be a good idea to increase this value to avoid dropping clients that didn't set their session property correctly. On the other hand in the presto works in environment where they are only very short queries this value set to small value may be used to detect user errors in queries. It may also be decreased in poor presto cluster configuration with mostly short queries to increase garbage collection efficiency and by that lowering memory usage in cluster.


``query.queue-config-file``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String``
 * **Default value:**
 * **Description:** This property may be defined to provide patch to queue config file. This is new way of providing such informations as ``query.max-concurrent-queries`` and ``query.max-queued-queries``. The file should contain JSON configuration described in :doc:`/admin/queue`.


``query.remote-task.max-callback-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``1000``
 * **Description:** This value describe max size of thread pool used to handle HTTP requests responses for task in cluster. Higher value will cause more of resources to be used for handling HTTP communication itself though increasing this value may improve response time when presto is distributed across many hosts or there is a lot of small queries going on in the system.


``query.remote-task.min-error-duration``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration, at least ``1s``)
 * **Default value:** ``2 m``
 * **Description:** The minimal time that HTTP worker must be unavailable for server to drop the connection. Higher value may be recommended in unstable connection conditions. This value is only a bottom line so there is no guarantee that node will be considered dead after such amount of time. In order to consider node dead the defined time must pass between two failed attempts of HTTP communication, with no successful communication in between.


``query.schedule-split-batch-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``1000``
 * **Description:** The size of single data chunk expressed in rows that will be processed as single split. Higher value may be used if system works in reliable environment and there the responsiveness is less important then average answer time. Decreasing this value may have a positive effect if there are lots of nodes in system and calculations are relatively heavy for each of rows. Other scenario may be if there are many nodes with poor stability - lowering this number will allow to react faster and for that reason the lost computation time will be potentially lower.


.. _tuning-pref-task:

Tasks managment properties
--------------------------


.. _task-concurrency:

``task.concurrency``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``1``
 * **Description:** Default local concurrency for parallel operators. Serves as default value for ``task_concurrency`` session property. Increasing this value is strongly recommended when any of CPU, IO or memory is not saturated on regular basis. In this scenario it will allow queries to utilize as many resources as possible. Setting this value to high will cause queries to slow down. It may happen even if none of resources is saturated as there are cases in which increasing parallelism is not possible due to algorithms limitations.


``task.http-response-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``100``
 * **Description:** Max number of threads that may be created to handle http responses. Threads are created on demand and they ends when there is no response to be sent. That means that there is no overhead if there is only a small number of request handled by system even if this value is big. On the other hand increasing this value may increase utilization of CPU in multicore environment (with the cost of memory usage). Also in systems having a lot of requests, the response time distribution may be manipulated using this property. Higher value may be used to avoid outliers adding the cost of increased average response time.


``task.http-timeout-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``3``
 * **Description:** Number of threads spawned for handling timeouts of http requests. Presto server sends update of query status whenever it is different then the one that client knows about. However in order to ensure client that connection is still alive, server sends this data after delay declared internally in HTTP headers (by default ``200 ms``). This property tells how many threads are designated to handle this delay. If the property turn out to low it's possible that the update time will increase even significantly when comparing to requested value (``200ms``). Increasing this value may solve the problem, but it generate a cost of additional memory even if threads are not used all the time. If there is no problem with updating status of query this value should not be manipulated.


``task.info-update-interval``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration)
 * **Default value:** ``200 ms``
 * **Description:** Controls staleness of task information which is used in scheduling. Increasing this value can reduce coordinator CPU load but may result in suboptimal split scheduling.


``task.max-index-memory``
^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``64 MB``
 * **Description:** Max size of index cache in memory used for index based joins. Increasing this value allows to use more memory for such queries which may improve time of huge table joins.


``task.max-partial-aggregation-memory``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``16 MB``
 * **Description:** Max size of partial aggregation result (if it is splitable). Increasing this value will decrease fragmentation of result which may improve general times and CPU utilization with the cost of additional memory usage. Also high value of this property may cause drop in performance in unstable cluster conditions.


``task.max-worker-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``Node CPUs`` * ``2``
 * **Description:** Sets the number of threads used by workers to process splits. Increasing this number can improve throughput if worker CPU utilization is low and all the threads are in use, but will cause increased heap space usage. The number of active threads is available via the ``com.facebook.presto.execution.TaskExecutor.RunningSplits`` JMX stat.


``task.min-drivers``
^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``Node CPUs`` * ``4``
 * **Description:** This describes how many drivers are kept on worker any time (if there is anything to do). The smaller value may cause better responsiveness for new task but possibly decreases CPU utilization. Higher value makes context switching faster with the cost of additional memory. The general rules of managing drivers is that if there is possibility of assigning a split to driver it is assigned if: there are less then ``3`` drivers assigned to given task OR there is less drivers on worker then ``task.min-drivers`` OR the task has been enqueued with ``force start`` property.


``task.operator-pre-allocated-memory``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``16 MB``
 * **Description:** Memory preallocated for each driver in query execution. Increasing this value may cause less efficient memory usage but allows to fail fast in low memory environment more frequently.


``task.share-index-loading``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** It allows to control whether index lookups join has index shared within a task. This enables the possibility of optimizing for index cache hits or for more CPU parallelism depending on the property value. Serves as default for ``task_share_index_loading`` session property.


``task.writer-count``
^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``1``
 * **Description:** Describes how many parallel writers may try to access I/O while executing queries in session. Serves as default for session property ``task_writer_count``. Setting this value to higher than default may increase write speed especially when query is NOT IO bounded and could use of more CPU cores for parallel writes. However in many cases increasing this value will visibly increase computation time while writing.



.. _tuning-pref-session:

Session properties
------------------

``processing_optimization``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (``disabled``, ``columnar`` or ``columnar_dictionary``)
 * **Default value:** ``optimizer.processing-optimization`` (``false``)
 * **Description:** See :ref:`optimizer.processing-optimization<tuning-pref-optimizer>`.


``execution_policy``
^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (``all-at-once`` or ``phased``)
 * **Default value:** ``query.execution-policy`` (``all-at-once``)
 * **Description:** See :ref:`query.execution-policy <tuning-pref-query>`.


``hash_partition_count``
^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``query.initial-hash-partitions`` (``100``)
 * **Description:** See :ref:`query.initial-hash-partitions <tuning-pref-query>`.


``optimize_hash_generation``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``optimizer.optimize-hash-generation`` (``true``)
 * **Description:** See :ref:`optimizer.optimize-hash-generation <tuning-pref-optimizer>`.


``orc_max_buffer_size``
^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``hive.orc.max-buffer-size`` (``8 MB``)
 * **Description:** See :ref:`hive.orc.max-buffer-size <tuning-pref-hive>`.


``orc_max_merge_distance``
^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``hive.orc.max-merge-distance`` (``1 MB``)
 * **Description:** See :ref:`hive.orc.max-merge-distance <tuning-pref-hive>`.


``orc_stream_buffer_size``
^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``hive.orc.max-buffer-size`` (``8 MB``)
 * **Description:** See :ref:`hive.orc.max-buffer-size <tuning-pref-hive>`.


``plan_with_table_node_partitioning``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:** **Experimental.** Adapt plan to use backend partitioning. By setting this property you allow to use partitioning provided by table layout itself while collecting required data. This may allow to utilize optimization of table layout provided by specific connector. In particular, when this is set presto will try to partition data for workers in a way that each workers gets a chunk of data that comes from one backend partition. It can be particularly useful due to the I/O distribution optimization in table partitioning. Note that this property may only be utilized if given projection uses all columns used for table partitioning inside connector.


``prefer_streaming_operators``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Prefer source table layouts that produce streaming operators. Setting this property will allow workers not to wait for chunks of data to start processing them while scanning tables. This may cause faster processing  with lower latency and downtime but some operators may do things more efficiently when working with chunks of data.


``push_table_write_through_union``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``optimizer.push-table-write-through-union`` (``true``)
 * **Description:** See :ref:`optimizer.push-table-writethrough-union <tuning-pref-optimizer>`.


``query_max_memory``
^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``query.max-memory`` (``20 GB``)
 * **Description:** This property can be use to be nice to the cluster if a particular query is not as important as the usual cluster routines. Setting this value to less than the server property ``query.max-memory`` will cause presto to drop the query in the session if it will require more then ``query_max_memory`` memory. Setting this value to higher than ``query.max-memory`` will not have any effect.



``query_max_run_time``
^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration)
 * **Default value:** ``query.max-run-time`` (``100 d``)
 * **Description:** If the expected query processing time is higher than ``query.max-run-time``, it is crucial to set this session property to prevent results of long running queries being dropped after ``query.max-run-time``. A session may also set this value to lower than ``query.max-run-time`` in order to crosscheck for bugs in query. It may be particularly useful when setting up a session with a very large number of short-running queries. It is important to set this value to much higher than the average query time to avoid problems with outliers (some queries may randomly take much longer due to cluster load and other circumstances).


``resource_overcommit``
^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Use resources that are not guaranteed to be available to a query. This property allows you to exceed the limits of memory available per query and session. It may allow resources to be used more efficiently, but may also cause non-deterministic query drops due to insufficient memory on machine. It can be particularly useful for performing more demanding queries.


``task_concurrency``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``task.concurrency`` (``1``)
 * **Description:** Default number of local parallel aggregation jobs per worker. See :ref:`task.concurrency<task-concurrency>`.


``task_writer_count``
^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``task.writer-count`` (``1``)
 * **Description:** See :ref:`task.writer-count <tuning-pref-task>`.



JVM Settings
------------

The following can be helpful for diagnosing GC issues:

.. code-block:: none

    -XX:+PrintGCApplicationConcurrentTime
    -XX:+PrintGCApplicationStoppedTime
    -XX:+PrintGCCause
    -XX:+PrintGCDateStamps
    -XX:+PrintGCTimeStamps
    -XX:+PrintGCDetails
    -XX:+PrintReferenceGC
    -XX:+PrintClassHistogramAfterFullGC
    -XX:+PrintClassHistogramBeforeFullGC
    -XX:PrintFLSStatistics=2
    -XX:+PrintAdaptiveSizePolicy
    -XX:+PrintSafepointStatistics
    -XX:PrintSafepointStatisticsCount=1
