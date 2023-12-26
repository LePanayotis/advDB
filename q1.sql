SELECT
    year,
    month,
    crime_total,
    ROW_NUMBER() OVER (ORDER BY year, crime_total DESC) AS num
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY year ORDER BY crime_total) AS order_within_year
    FROM (
        SELECT
            YEAR(`Date Rptd`) AS year,
            MONTH(`Date Rptd`) AS month,
            COUNT(*) AS crime_total
        FROM crime_data
        GROUP BY year, month
        ORDER BY year, crime_total DESC
    ) ranked
    WHERE order_within_year <= 3
) ranked_final;
