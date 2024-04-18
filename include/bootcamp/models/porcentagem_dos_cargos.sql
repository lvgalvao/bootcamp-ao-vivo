{{ config(materialized='table', schema='gold', alias='porcentagem_dos_cargos') }}

WITH somente_da_area AS (
  SELECT *
  FROM {{ ref('somente_da_area') }}
),
aggregations AS (
  SELECT
    cargo,
    COUNT(*) AS count_registros,
    CAST(COUNT(*) AS FLOAT) / (SELECT COUNT(*) FROM somente_da_area) * 100 AS porcentagem
  FROM somente_da_area
  GROUP BY cargo
)
SELECT *
FROM aggregations