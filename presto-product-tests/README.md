# Presto product tests

## Test configuration

Test assume that you have hadoop and presto cluster running. 

To make it work you need either: 
 - define environment variables for hadodp ${HADOOP_MASTER} and presto ${PRESTO_MASTER} with their IP numbers,
and ${PRESTO_PRODUCT_TESTS_ROOT} with path to the presto product test directory
 - create src/test/resources/test-configuration-local.yaml with following (example content):

```
databases:
  hive:
    host: 172.16.2.10
    jdbc_jar: /home/kogut/src/git/teradata/presto/presto-product-tests/libs/test-framework-hive-jdbc-all.jar

  presto:
    host: 192.168.205.1
```

## Running tests

Product tests are not run by default. To start them use _productTests_ maven profile:

```
mvn -PproductTests test
```

## Running particular test groups

```
mvn -PproductTests -Dgroups=jmx test
```
