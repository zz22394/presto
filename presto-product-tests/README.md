# Presto product tests

## Running tests

Product tests are not run by default. To start them use _productTests_ maven profile:

```
cp presto-product-tests/src/test/resources/test-configuration.yaml.example presto-product-tests/src/test/resources/test-configuration.yaml
vim presto-product-tests/src/test/resources/test-configuration.yaml
# edit configuration file according to your setup
mvn -PproductTests test
```
