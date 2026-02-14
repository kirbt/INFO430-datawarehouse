/*
QUERY 1 (CUBE)

The purpose of this query is to make a multi-dimensional summary of total
aid by organization type, sector, and year using a CUBE function

This will allow policy makers to see which types of organizations
(e.g. Multilateral, Government) fund which sectors overtime.
*/

-- place cube function into table
SELECT 
    o.organization_type_name AS org_type,
    s.sector_name,
    t.year,
    SUM(f.value_usd) AS total_aid
INTO #orgtype_sector_year_cube
FROM fact_aid_transaction f
JOIN dim_organization o ON f.reporting_org_id = o.organization_id
JOIN dim_sector s ON f.sector_id = s.sector_id
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY CUBE (o.organization_type_name, s.sector_name, t.year);

-- query cube
SELECT 
    ISNULL(org_type, 'All Org Types') AS organization_type,
    ISNULL(sector_name, 'All Sectors') AS sector,
    ISNULL(CAST(year AS VARCHAR(4)), 'All Years') AS year,
    FORMAT(total_aid, 'C0') AS total_aid_usd
FROM #orgtype_sector_year_cube
ORDER BY organization_type, sector, year;


/* 
QUERY 2 (CUBE)

The purpose of this query is to make a multi-dimensional summary of
total aid disbursements by country, sector, and year using a CUBE
function.

This will allow policy makers to what sectors dominate
in terms of overall funding, and see which countries receive the most
support across all sectors.

*/

-- place cube function into temp table
SELECT 
    c.country_name,
    s.sector_name,
    t.year,
    SUM(f.value_usd) AS total_aid
INTO #country_sector_year_cube
FROM fact_aid_transaction f
JOIN dim_country c ON f.country_id = c.country_id
JOIN dim_sector s ON f.sector_id = s.sector_id
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY CUBE (c.country_name, s.sector_name, t.year);

-- query cube
SELECT 
    ISNULL(country_name, 'All Countries') AS Country,
    ISNULL(sector_name, 'All Sectors') AS Sector,
    ISNULL(CAST(year AS VARCHAR(4)), 'All Years') AS "Total Aid",
    FORMAT(total_aid, 'N0') AS total_aid_usd
FROM #country_sector_year_cube
ORDER BY country, sector, year;


/* 
QUERY 3 (Ranking Window Function)

The purpose of this queyr is to rank org types within each sector by the
total amount of aid they have disbursed. This would highly which types
of donors (Government, multilateral, etc.) dominate funding in specific
sectors. 

This is useful for understanding donor specialization. For example,
maybe multilateral organizations are dominant in the administrative costs
sector

*/

WITH org_type_sector_total AS (
    SELECT 
        s.sector_name,
        o.organization_type_name AS org_type,
        SUM(f.value_usd) AS total_aid
    FROM fact_aid_transaction f
    JOIN dim_sector s ON f.sector_id = s.sector_id
    JOIN dim_organization o ON f.reporting_org_id = o.organization_id
    GROUP BY s.sector_name, o.organization_type_name
),
ranked AS (
    SELECT 
        sector_name,
        org_type,
        total_aid,
        DENSE_RANK() OVER (
            PARTITION BY sector_name 
            ORDER BY total_aid DESC
        ) AS org_type_rank
    FROM org_type_sector_total
)
SELECT 
    sector_name,
    org_type,
    FORMAT(total_aid, 'C0') AS total_aid_usd,
    org_type_rank
FROM ranked
WHERE org_type_rank <= 3
ORDER BY sector_name, org_type_rank;


/* 
QUERY 4 (Ranking Window Function)

The purpose of this query is to rank countries in each organization type
by the total amount of aid they receive. This highlights which countries
might be the primary receivers of funding from different types of donors.

This query would be useful for understanding relationships between donors
and countries.
*/

WITH orgtype_country_totals AS (
    SELECT 
        o.organization_type_name AS org_type,
        c.country_name,
        SUM(f.value_usd) AS total_aid
    FROM fact_aid_transaction f
    JOIN dim_organization o ON f.reporting_org_id = o.organization_id
    JOIN dim_country c ON f.country_id = c.country_id
    GROUP BY o.organization_type_name, c.country_name
),
ranked AS (
    SELECT 
        org_type,
        country_name,
        total_aid,
        RANK() OVER (
            PARTITION BY org_type
            ORDER BY total_aid DESC
        ) AS country_rank
    FROM orgtype_country_totals
)
SELECT 
    org_type,
    country_name,
    FORMAT(total_aid, 'C0') AS total_aid_usd,
    country_rank
FROM ranked
WHERE country_rank <= 5
ORDER BY org_type, country_rank;


/* 
QUERY 4 (Ranking Window Function)

The purpose of this query is to rank countries within each organization 
type by the total amount of aid they receive. This highlights which 
countries are the primary recipients of funding from different types of 
donors (e.g., Government, Multilateral, NGO).

This is useful for understanding donor-country relationships. For example,
government donors may concentrate funding in neighboring countries, while
multilateral organizations may distribute funding more globally.
*/

WITH orgtype_country_totals AS (
    SELECT 
        o.organization_type_name AS org_type,
        c.country_name,
        SUM(f.value_usd) AS total_aid
    FROM fact_aid_transaction f
    JOIN dim_organization o ON f.reporting_org_id = o.organization_id
    JOIN dim_country c ON f.country_id = c.country_id
    GROUP BY o.organization_type_name, c.country_name
),
ranked AS (
    SELECT 
        org_type,
        country_name,
        total_aid,
        RANK() OVER (
            PARTITION BY org_type
            ORDER BY total_aid DESC
        ) AS country_rank
    FROM orgtype_country_totals
)
SELECT 
    org_type,
    country_name,
    FORMAT(total_aid, 'C0') AS total_aid_usd,
    country_rank
FROM ranked
WHERE country_rank <= 5
ORDER BY org_type, country_rank;

/* 
QUERY 5 (Value Window Function)

The purpose of this query is to calculate year over year changes in
total aid received by each country, using the LAG() window function,
specifically in this case for Afghanistan.

This would allow policy makers to identify whether a country is
experiencing increases or descreases in support overtime.
*/

WITH country_year_totals AS (
    SELECT 
        c.country_name,
        t.year,
        SUM(f.value_usd) AS total_aid
    FROM fact_aid_transaction f
    JOIN dim_country c ON f.country_id = c.country_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE c.country_name = 'Afghanistan'
    GROUP BY c.country_name, t.year
),
with_lag AS (
    SELECT 
        country_name,
        year,
        total_aid,
        LAG(total_aid) OVER (
            PARTITION BY country_name
            ORDER BY year
        ) AS prev_year_aid
    FROM country_year_totals
)
SELECT 
    country_name AS 'Country',
    year AS "Year",
    FORMAT(total_aid, 'C0') AS "Aid",
    FORMAT(prev_year_aid, 'C0') AS "Previous Aid ",
    FORMAT(total_aid - prev_year_aid, '#,0') AS "YoY Change in Aid"
FROM with_lag
ORDER BY country_name, year;


/* 
QUERY 6 (Value Window Function)

The purpose of this query is to calculate projected year over year
changes in total aid disbursements for each organization type, using 
LEAD()

Unlike LAG(), which looks backward, LEAD() looks forward, which would
allow policy makers to identify whether specific donor types might
increase or decrease in donations over the following year.

This is useful for anticipating shifts in donor behavior and planning 
resource allocation accordingly.
*/

WITH orgtype_year_totals AS (
    SELECT 
        o.organization_type_name AS org_type,
        t.year,
        SUM(f.value_usd) AS total_aid
    FROM fact_aid_transaction f
    JOIN dim_organization o ON f.reporting_org_id = o.organization_id
    JOIN dim_time t ON f.time_id = t.time_id
    GROUP BY o.organization_type_name, t.year
),
with_lead AS (
    SELECT 
        org_type,
        year,
        total_aid,
        LEAD(total_aid) OVER (
            PARTITION BY org_type
            ORDER BY year
        ) AS next_year_aid
    FROM orgtype_year_totals
)
SELECT 
    org_type,
    year,
    FORMAT(total_aid, 'C0') AS total_aid_usd,
    FORMAT(next_year_aid, 'C0') AS next_year_usd,
    FORMAT(next_year_aid - total_aid, '#,0') AS projected_change_usd
FROM with_lead
ORDER BY org_type, year;

/* 
QUERY 7 (Time Series Analytics)

The purpose of this query is to calculate a 3-year moving average of 
total aid for each sector. A moving average would smooth out
year to year differences in funding and allow policy makers to 
identify long-term funding trends rather than short-term.
*/

WITH sector_year_totals AS (
    SELECT 
        s.sector_name,
        t.year,
        SUM(f.value_usd) AS total_aid
    FROM fact_aid_transaction f
    JOIN dim_sector s ON f.sector_id = s.sector_id
    JOIN dim_time t ON f.time_id = t.time_id
    GROUP BY s.sector_name, t.year
)
SELECT 
    sector_name AS "Sector",
    year AS "Year",
    FORMAT(total_aid, 'C0') AS "Aid",
    FORMAT(
        AVG(total_aid) OVER (
            PARTITION BY sector_name
            ORDER BY year
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 'C0'
    ) AS "3-Year Moving Average"
FROM sector_year_totals
ORDER BY sector_name, year;

/* 
QUERY 8 (Time Series Analytics)

The purpose of this query is to calculate a 3-year moving average of
GDP per capita for Bangladesh. Using a moving average would smooth out
short-term fluctuations so that policymakers can identify long-term economic
trends.
*/

WITH gdp_yearly AS (
    SELECT 
        c.country_name,
        t.year,
        f.gdp_per_capita
    FROM fact_country_context f
    JOIN dim_country c ON f.country_id = c.country_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE c.country_name = 'Bangladesh'
)
SELECT 
    country_name AS "Country",
    year AS "Year",
    FORMAT(gdp_per_capita, 'N2') AS "GDP per Capita",
    FORMAT(
        AVG(gdp_per_capita) OVER (
            PARTITION BY country_name
            ORDER BY year
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 'N2'
    ) AS "3-Year Moving Average"
FROM gdp_yearly
ORDER BY country_name, year;
