
# ETL and Data Warehouse Project for INFO 430

Transforms IATI Country Development Finance Data + World Bank WDI into a star schema data warehouse for aid analytics.

# Data Sources
| Source            | URL                          | Contents                                           |
| ----------------- | ---------------------------- | -------------------------------------------------- |
| IATI Country Data | countrydata.iatistandard.org | 100K+ aid transactions (orgs, sectors, USD values) |
| World Bank WDI    | databank.worldbank.org       | Country-year indicators (1970-2024)                |

# Schema Output
```
data/etl_output/
├── dim_time.csv              # time_id, year, quarter
├── dim_country.csv           # country_id, iso_code, country_name
├── dim_sector.csv            # sector_id, sector_code, sector_name, category
├── dim_organization.csv      # org_id, org_name, org_type, role
├── dim_aid_type.csv          # aid_type_id, aid_type_code, aid_type_name
├── dim_transaction_type.csv  # transaction_type_id, code, name
├── fact_aid_transaction.csv  # iati_id, value_usd, humanitarian + FKs
└── fact_country_context.csv  # country_id, time_id, population, gdp_per_capita...
```

# Sample Query

```sql

WITH aid_by_year AS (
    SELECT 
        c.country_id,
        c.country_name,
        t.year,
        SUM(f.value_usd) AS total_aid
    FROM fact_aid_transaction f
    JOIN dim_country c ON f.country_id = c.country_id
    JOIN dim_time t ON f.time_id = t.time_id
    GROUP BY c.country_id, c.country_name, t.year
),

-- get population from country context
population_by_year AS (
    SELECT 
        c.country_id,
        c.country_name,
        t.year,
        f.population
    FROM fact_country_context f
    JOIN dim_country c ON f.country_id = c.country_id
    JOIN dim_time t ON f.time_id = t.time_id
),

-- combine the two tables
aid_per_capita AS (
    SELECT 
        a.country_name,
        a.year,
        a.total_aid,
        p.population,
        CASE 
            WHEN p.population > 0 THEN a.total_aid * 1.0 / p.population
            ELSE NULL
        END AS aid_per_capita
    FROM aid_by_year a
    JOIN population_by_year p 
        ON a.country_id = p.country_id
       AND a.year = p.year
)

-- compute a 3 year moving avg
SELECT 
    country_name AS "Country",
    year AS "Year",
    FORMAT(aid_per_capita, 'C4') AS "Aid per Capita",
    FORMAT(
        AVG(aid_per_capita) OVER (
            PARTITION BY country_name
            ORDER BY year
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 'C4'
    ) AS "3 Year Moving Average"
FROM aid_per_capita
ORDER BY country_name, year;
```
| Country     | Year | Aid per Capita | 3‑Year Moving Average |
|-------------|------|----------------|------------------------|
| Afghanistan | 1970 | $0.8484        | $0.8484               |
| Afghanistan | 1997 | $0.0264        | $0.4374               |
| Afghanistan | 1998 | $2.7748        | $1.2166               |
| Afghanistan | 1999 | $1.6852        | $1.4955               |
| Afghanistan | 2000 | $2.0031        | $2.1544               |

# Credits
IATI: countrydata.iatistandard.org

World Bank: databank.worldbank.org


