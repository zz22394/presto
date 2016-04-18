-- type: jdbc
CREATE TABLE IF NOT EXISTS %NAME% (
  c_bigint BIGINT,
  c_double DOUBLE,
  c_decimal DECIMAL, 
  c_decimal_with_params DECIMAL(10, 5),
  c_timestamp TIMESTAMP, 
  c_date DATE, 
  c_varchar VARCHAR,
  c_varchar_with_param VARCHAR(10),
  c_boolean BOOLEAN,
  c_varbinary VARBINARY
)
WITH (FORMAT='PARQUET')
