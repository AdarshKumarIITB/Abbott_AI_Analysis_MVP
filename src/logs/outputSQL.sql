/* ---------------------------------------------------------------
   One-statement script that returns a SINGLE result-set combining
   • the top territories that explain ~80 % (or top-10) of the gap
   • the brands inside those territories that explain ~80 % of each gap
   --------------------------------------------------------------- */

WITH
-- 1️⃣  Base aggregation at Brand × Territory
gap_base AS (
    SELECT
        Terr_Code,
        Brand,
        SUM(Tgt_Value)  AS tgt_val,
        SUM(Sec_Value)  AS sec_val,
        SUM(Prim_Value) AS prim_val
    FROM analyzer
    WHERE Mth  = 'Apr'          -- month filter
      AND Zone = 'DELHI'        -- zone filter
      AND Mth <> 'All'
    GROUP BY Terr_Code, Brand
),

-- 2️⃣  Metric calculation & under-performance filter
brand_gap AS (
    SELECT
        Terr_Code,
        Brand,
        tgt_val - prim_val AS non_performance_gap,
        CASE
            WHEN tgt_val > 0 THEN (prim_val / tgt_val) * 100
            ELSE 0
        END AS achievement_pct
    FROM gap_base
    WHERE (CASE
              WHEN tgt_val > 0 THEN (prim_val / tgt_val) * 100
              ELSE 0
           END) < 100
),

-- 3️⃣  Gap totals per territory
territory_tot AS (
    SELECT
        Terr_Code,
        SUM(non_performance_gap) AS terr_gap
    FROM brand_gap
    GROUP BY Terr_Code
),

-- 4️⃣  Rank territories and keep until 80 % (or 10 rows)
top_territory AS (
    SELECT
        Terr_Code,
        terr_gap,
        terr_gap / SUM(terr_gap) OVER ()                       AS terr_contrib_pct,
        SUM(terr_gap) OVER (ORDER BY terr_gap DESC)
          / SUM(terr_gap) OVER ()                              AS terr_cume_pct,
        ROW_NUMBER() OVER (ORDER BY terr_gap DESC)             AS rn
    FROM territory_tot
),

-- 5️⃣  Brands that make up 80 % of gap inside each selected territory
brand_within AS (
    SELECT
        bg.Terr_Code,
        bg.Brand,
        bg.non_performance_gap,
        bg.non_performance_gap
            / SUM(bg.non_performance_gap) OVER (PARTITION BY bg.Terr_Code)
            AS brand_contrib_pct,
        SUM(bg.non_performance_gap)
            OVER (PARTITION BY bg.Terr_Code
                   ORDER BY bg.non_performance_gap DESC)
          / SUM(bg.non_performance_gap) OVER (PARTITION BY bg.Terr_Code)
            AS brand_cume_pct
    FROM brand_gap bg
    JOIN top_territory tt
      ON bg.Terr_Code = tt.Terr_Code
    WHERE tt.terr_cume_pct <= 0.80   -- keep the same territories
       OR tt.rn <= 10
)

-- 6️⃣  SINGLE unified result-set
SELECT
    'Territory'                    AS level,
    tt.Terr_Code                   AS terr_code,
    NULL                           AS brand,
    tt.terr_gap                    AS gap_value,
    tt.terr_contrib_pct            AS contrib_pct,
    tt.terr_cume_pct               AS cume_pct
FROM top_territory tt
WHERE tt.terr_cume_pct <= 0.80
   OR tt.rn <= 10

UNION ALL

SELECT
    'Brand'                        AS level,
    bw.Terr_Code                   AS terr_code,
    bw.Brand                       AS brand,
    bw.non_performance_gap         AS gap_value,
    bw.brand_contrib_pct           AS contrib_pct,
    bw.brand_cume_pct              AS cume_pct
FROM brand_within bw
WHERE bw.brand_cume_pct <= 0.80

ORDER BY
    level,                -- Territories first, then Brands
    terr_code,
    gap_value DESC;
