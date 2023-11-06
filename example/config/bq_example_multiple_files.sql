-- Example data in place of an actual table of values.
-- MetricName: firstmetric
WITH example_data as (
    SELECT "ssa" as label, 5 as widgets
)

SELECT
   STRING(current_datetime) as date, SUM(widgets) as value
FROM
   example_data
GROUP BY
   date;


-- Example data in place of an actual table of values.
-- MetricName: fiiimetric
WITH example_data as (
    SELECT "b" as label, 5 as widgets,
)

SELECT
   STRING(current_datetime) as date, SUM(widgets) as value
FROM
   example_data
GROUP BY
   date;
