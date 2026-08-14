-- TPC-H Q11 (qualification parameters)
SELECT
   ps_partkey,
   SUM(ps_supplycost * ps_availqty) AS VALUE
FROM
   partsupp,
   supplier,
   nation
WHERE
   ps_suppkey = s_suppkey
   AND s_nationkey = n_nationkey
   AND n_name = 'GERMANY'
GROUP BY
   ps_partkey
HAVING
   SUM(ps_supplycost * ps_availqty) > (
   SELECT
      SUM(ps_supplycost * ps_availqty) * 0.0001 -- FRACTION = 0.0001 / SF (this file is the SF=1 instantiation)
   FROM
      partsupp, supplier, nation
   WHERE
      ps_suppkey = s_suppkey
      AND s_nationkey = n_nationkey
      AND n_name = 'GERMANY' )
   ORDER BY
      VALUE DESC;
