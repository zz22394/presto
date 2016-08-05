-- database: presto; groups: aggregate,quarantine; tables: lineitem
-- delimiter: |; types: BIGINT 
--! 
CREATE OR REPLACE VIEW revenue AS
  SELECT
    l_suppkey as supplier_no,
    sum(l_extendedprice * (1 - l_discount)) as total_revenue
  FROM
    lineitem
  WHERE
    l_shipdate >= DATE '1996-01-01'
    AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
GROUP BY
  l_suppkey;

SELECT
  count(*)
FROM
  revenue
WHERE
  total_revenue = (SELECT max(total_revenue) FROM revenue)
--! 
1
