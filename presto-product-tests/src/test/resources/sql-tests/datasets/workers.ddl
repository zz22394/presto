CREATE TABLE %NAME% (
  id_employee        INT,
  first_name         VARCHAR(255),
  last_name          VARCHAR(255),
  date_of_employment VARCHAR(255),
  department         INT,
  id_department      INT,
  name               VARCHAR(255),
  salary             INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '%LOCATION%'
TBLPROPERTIES('serialization.null.format'='#')
