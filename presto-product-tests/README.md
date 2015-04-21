# Presto product tests

## Test configuration

Test assume that you have hadoop and presto cluster running. Copy sample configuration
and adjust it accordingly:

```
cp src/test/resources/test-configuration.yaml.example src/test/resources/test-configuration.yaml
vim src/test/resources/test-configuration.yaml
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