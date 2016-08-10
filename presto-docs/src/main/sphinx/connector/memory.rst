==================
Memory Connector
==================

The Memory connector stores all data and metadata in RAM on workers
and both are discarded when Presto restarts.

.. warning::

    This connector will not work properly with multiple coordinators,
    since each coordinator will have a different metadata.

.. warning::

    This connector does not support ``DROP TABLE`` or any other method
    to release held resources. Once table is populated it will consume
    memory until next Presto restart.

Configuration
-------------

To configure the Memory connector, create a catalog properties file
``etc/catalog/memory.properties`` with the following contents:

.. code-block:: none

    connector.name=memory

Examples
--------

Create a table using the Memory connector::

    CREATE TABLE memory.default.nation AS
    SELECT * from tpch.tiny.nation;

Insert data into a table in the Memory connector::

    INSERT INTO memory.default.nation
    SELECT * FROM tpch.tiny.nation;

Select from the Memory connector::

    SELECT * FROM memory.default.nation;
