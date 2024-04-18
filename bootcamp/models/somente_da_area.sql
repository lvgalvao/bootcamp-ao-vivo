WITH filtered AS (
  SELECT *
  FROM {{ source('bronze', 'google_sheets_pesquisa') }}
  WHERE cargo NOT IN (
      'Interessado na área de dados',
      'Não trabalho na área, apenas me inscrevi pra conhecer mais sobre',
      'Em migração de carreira para dados',
      'Estudioso da área'
  )
)
SELECT *
FROM filtered