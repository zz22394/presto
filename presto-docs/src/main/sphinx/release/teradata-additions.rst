==================
Teradata Additions
==================

The following are notable additions that Teradata has added to Facebook's 0.141 release of presto

Prepared Statements
-------------------
Add support for Prepared statements and parameters via sql syntax.

    * :ref:`prepare`
    * :ref:`deallocate-prepare`
    * :ref:`execute`
    * :ref:`describe-input`
    * :ref:`describe-output`


Regular Expressions
-------------------
Add support for running regular expression functions using the more efficent re2j-td library by setting the session
variable ``regex_library`` to RE2J.  The memory footprint can be adjusted by setting ``re2j_dfa_states_limit``.
Additionally, the number of times the re2j library falls back from its DFA algorithm to the NFA algorithm (due to
hitting the states limit) before immediately starting with the NFA algorithm can be set with the ``re2j_dfa_retries``
session variable.

Kerberos Support
----------------
Add support for Presto to query from a Kerberized Hadoop cluster. The Hive connector provides additonal security options to support Hadoop clusters that have been configured to use Kerberos. When accessing HDFS, Presto can impersonate the end user who is running the query. This can be used with HDFS permissions and ACLs to provide additional security for data. See :ref:`security`

EXPLAIN ANALYZE
---------------
Execute the statement and show the distributed execution plan of the statement along with the cost of each operation. See :ref:`explain-analyze`
