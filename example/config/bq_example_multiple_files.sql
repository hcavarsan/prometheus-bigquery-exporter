-- Example data in place of an actual table of values.
-- MetricName: firstmetric
WITH example_data as (
    SELECT "ssa" as label, 5 as widgets
    UNION ALL
    SELECT "c" as label, 2 as widgets
    UNION ALL
    SELECT "dd" as label, 3 as widgets
)

SELECT
   label, SUM(widgets) as value
FROM
   example_data
GROUP BY
   label;

-- Example data in place of an actual table of values.
-- MetricName: fiiimetric
WITH example_data as (
    SELECT "a" as label, 5 as widgets
    UNION ALL
    SELECT "b" as label, 2 as widgets
    UNION ALL
    SELECT "b" as label, 3 as widgets
)

SELECT
   label, SUM(widgets) as value
FROM
   example_data
GROUP BY
   label;