USE fmcg_inflation_project;

-- ============================================================
-- FMCG INFLATION + MARGIN RISK CONSULTING PROJECT 
-- DATASETS USED:
-- 1. raw_material_prices_5000.csv  (Daily commodity prices)
-- 2. product_cost_structure_1000.csv (50 products master)
-- 3. regional_sales_10000.csv (Monthly regional sales with date)
-- ============================================================


-- ============================================================
-- 1. RESET ALL TABLES (CLEAN START)
-- ============================================================

DROP TABLE IF EXISTS raw_material_prices;
DROP TABLE IF EXISTS product_cost_structure;
DROP TABLE IF EXISTS regional_sales;
DROP TABLE IF EXISTS category_margin_benchmarks;


-- ============================================================
-- 2. CREATE TABLES (MATCHING CSV SCHEMA)
-- ============================================================

-- Table 1: Daily Commodity Prices
CREATE TABLE raw_material_prices (
    date DATE,
    material VARCHAR(50),
    price_per_kg_inr FLOAT
);

-- Table 2: Product Cost Structure (50 Products)
CREATE TABLE product_cost_structure (
    product VARCHAR(100),
    category VARCHAR(50),
    primary_material VARCHAR(50),
    material_cost_pct FLOAT,
    packaging_cost_pct FLOAT,
    logistics_cost_pct FLOAT,
    base_price_inr FLOAT
);

-- Table 3: Monthly Regional Sales (Time-Series)
CREATE TABLE regional_sales (
    date DATE,
    region VARCHAR(20),
    product VARCHAR(100),
    monthly_units_sold INT
);


-- ============================================================
-- 3. LOAD CSV FILES USING TABLE DATA IMPORT WIZARD
-- ============================================================
-- Import these manually:
-- raw_material_prices_5000.csv → raw_material_prices
-- product_cost_structure_1000.csv → product_cost_structure
-- regional_sales_10000.csv → regional_sales


/*3. CONSULTING BENCHMARK TABLE (CATEGORY MARGINS)*/
--
-- Consultants estimate product cost using category-level gross
-- margin benchmarks from annual reports + FMCG industry ranges.
-- ============================================================

CREATE TABLE category_margin_benchmarks (
    category VARCHAR(50),
    assumed_margin_pct FLOAT
);

INSERT INTO category_margin_benchmarks VALUES
('Dairy', 0.40),
('Snacks', 0.45),
('Chocolate', 0.50),
('Beverage', 0.55),
('Instant Food', 0.45),
('Nutrition', 0.48);


-- ============================================================
-- 4. COMMODITY INFLATION DRIVERS + RISK LEVEL
-- PURPOSE: Identify highest inflation commodities
-- ============================================================
WITH start_end AS (
    SELECT
        material,

        FIRST_VALUE(price_per_kg_inr)
        OVER(PARTITION BY material ORDER BY date) AS start_price,

        LAST_VALUE(price_per_kg_inr)
        OVER(
            PARTITION BY material
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS end_price

    FROM raw_material_prices
)

SELECT DISTINCT
    material,
    start_price,
    end_price,

    ROUND(((end_price - start_price)/start_price)*100,2)
        AS inflation_percent,

    CASE
        WHEN ((end_price - start_price)/start_price)*100 >= 25 THEN 'HIGH'
        WHEN ((end_price - start_price)/start_price)*100 BETWEEN 15 AND 25 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS inflation_risk_level

FROM start_end
ORDER BY inflation_percent DESC;
-- ============================================================
-- 5. Revenue Exposure by Region 
-- PURPOSE: (Monthly Revenue Concentration)
-- ============================================================


SELECT
    r.date,
    r.region,
    r.product,

    r.monthly_units_sold,
    p.base_price_inr,

    ROUND(r.monthly_units_sold * p.base_price_inr,2)
        AS monthly_revenue

FROM regional_sales r
JOIN product_cost_structure p
ON r.product = p.product

ORDER BY monthly_revenue DESC
LIMIT 20;


-- ============================================================
-- 6. BENCHMARK UNIT ECONOMICS MODEL
-- PURPOSE: Estimate unit cost using margin benchmarks
-- ============================================================

SELECT
    p.product,
    p.category,
    p.base_price_inr AS unit_price,

    b.assumed_margin_pct,

    ROUND(p.base_price_inr * (1 - b.assumed_margin_pct),2)
        AS estimated_unit_cost

FROM product_cost_structure p
JOIN category_margin_benchmarks b
ON p.category = b.category;


-- ============================================================
-- 7. MARGIN EROSION DUE TO COMMODITY INFLATION
-- PURPOSE: Identify products losing profitability
-- ============================================================

WITH inflation AS (

    SELECT DISTINCT
        material,

        ROUND(((end_price - start_price)/start_price)*100,2)
            AS inflation_percent

    FROM (
        SELECT
            material,

            FIRST_VALUE(price_per_kg_inr)
            OVER(PARTITION BY material ORDER BY date) AS start_price,

            LAST_VALUE(price_per_kg_inr)
            OVER(
                PARTITION BY material
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS end_price

        FROM raw_material_prices
    ) x
),

unit_costs AS (

    SELECT
        p.product,
        p.category,
        p.primary_material,
        p.base_price_inr AS unit_price,

        ROUND(p.base_price_inr * (1 - b.assumed_margin_pct),2)
            AS estimated_unit_cost

    FROM product_cost_structure p
    JOIN category_margin_benchmarks b
    ON p.category = b.category
)

SELECT
    u.product,
    u.category,
    u.primary_material,

    i.inflation_percent,

    u.unit_price,
    u.estimated_unit_cost,

    ROUND(((u.unit_price - u.estimated_unit_cost)/u.unit_price)*100,2)
        AS base_margin_pct,

    ROUND(u.estimated_unit_cost * (1 + i.inflation_percent/100),2)
        AS inflated_unit_cost,

    ROUND(((u.unit_price -
          (u.estimated_unit_cost * (1 + i.inflation_percent/100)))
          / u.unit_price)*100,2)
        AS margin_after_inflation

FROM unit_costs u
JOIN inflation i
ON u.primary_material = i.material

ORDER BY margin_after_inflation ASC;

-- ============================================================
-- 8. PRICING RECOVERY SIMULATION (8% HIKE)
-- PURPOSE: Does price increase restore margins?
-- ============================================================

WITH inflation AS (
    SELECT DISTINCT
        material,
        ROUND(((end_price - start_price)/start_price)*100,2)
            AS inflation_percent
    FROM (
        SELECT
            material,
            FIRST_VALUE(price_per_kg_inr)
            OVER(PARTITION BY material ORDER BY date) AS start_price,
            LAST_VALUE(price_per_kg_inr)
            OVER(
                PARTITION BY material
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS end_price
        FROM raw_material_prices
    ) x
),

unit_costs AS (
    SELECT
        p.product,
        p.primary_material,
        p.base_price_inr AS unit_price,
        ROUND(p.base_price_inr * (1 - b.assumed_margin_pct),2)
            AS estimated_unit_cost
    FROM product_cost_structure p
    JOIN category_margin_benchmarks b
    ON p.category=b.category
)

SELECT
    u.product,

    ROUND(((u.unit_price -
          (u.estimated_unit_cost * (1 + i.inflation_percent/100)))
          / u.unit_price)*100,2)
        AS margin_after_inflation,

    ROUND((((u.unit_price * 1.08) -
          (u.estimated_unit_cost * (1 + i.inflation_percent/100)))
          / (u.unit_price * 1.08))*100,2)
        AS margin_after_8pct_hike

FROM unit_costs u
JOIN inflation i
ON u.primary_material=i.material;



-- ============================================================
-- 9. PROFIT AT RISK (₹ BUSINESS IMPACT)
-- PURPOSE: Quantify total profit loss over 3 years
-- ============================================================

WITH total_sales AS (
    SELECT
        product,
        SUM(monthly_units_sold) AS total_units
    FROM regional_sales
    GROUP BY product
),

unit_costs AS (
    SELECT
        p.product,
        p.base_price_inr AS unit_price,
        ROUND(p.base_price_inr * (1 - b.assumed_margin_pct),2)
            AS estimated_unit_cost
    FROM product_cost_structure p
    JOIN category_margin_benchmarks b
    ON p.category=b.category
)

SELECT
    s.product,
    s.total_units,

    ROUND(s.total_units * (u.unit_price - u.estimated_unit_cost),2)
        AS base_profit,

    ROUND(s.total_units * (u.unit_price - (u.estimated_unit_cost * 1.15)),2)
        AS profit_under_cost_shock,

    ROUND(
        (s.total_units * (u.unit_price - u.estimated_unit_cost)) -
        (s.total_units * (u.unit_price - (u.estimated_unit_cost * 1.15))),
    2) AS profit_loss_inr

FROM total_sales s
JOIN unit_costs u
ON s.product=u.product

ORDER BY profit_loss_inr DESC;


-- ============================================================
-- 10. POWER BI MASTER TABLE (FINAL SINGLE SOURCE)
-- PURPOSE: One export table for dashboards
-- ============================================================

WITH inflation AS (
    SELECT DISTINCT
        material,
        ROUND(((end_price - start_price)/start_price)*100,2)
            AS inflation_percent
    FROM (
        SELECT
            material,
            FIRST_VALUE(price_per_kg_inr)
            OVER(PARTITION BY material ORDER BY date) AS start_price,
            LAST_VALUE(price_per_kg_inr)
            OVER(
                PARTITION BY material
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS end_price
        FROM raw_material_prices
    ) x
),

unit_costs AS (
    SELECT
        p.product,
        p.category,
        p.primary_material,
        p.base_price_inr AS unit_price,

        ROUND(p.base_price_inr * (1 - b.assumed_margin_pct),2)
            AS estimated_unit_cost

    FROM product_cost_structure p
    JOIN category_margin_benchmarks b
    ON p.category=b.category
)

SELECT
    r.date,
    r.region,
    r.product,
    u.category,
    u.primary_material,

    r.monthly_units_sold,

    ROUND(r.monthly_units_sold * u.unit_price,2)
        AS monthly_revenue,

    i.inflation_percent,

    ROUND(((u.unit_price - u.estimated_unit_cost)/u.unit_price)*100,2)
        AS base_margin_pct,

    ROUND(((u.unit_price -
          (u.estimated_unit_cost * (1 + i.inflation_percent/100)))
          / u.unit_price)*100,2)
        AS margin_after_inflation,

    CASE
        WHEN (((u.unit_price -
              (u.estimated_unit_cost * (1 + i.inflation_percent/100)))
              / u.unit_price)*100) < 20
            THEN 'Immediate Price Revision'

        WHEN (((u.unit_price -
              (u.estimated_unit_cost * (1 + i.inflation_percent/100)))
              / u.unit_price)*100) BETWEEN 20 AND 30
            THEN 'Cost Optimization'

        ELSE 'Monitor'
    END AS consulting_action

FROM regional_sales r
JOIN unit_costs u ON r.product=u.product
JOIN inflation i ON u.primary_material=i.material;
