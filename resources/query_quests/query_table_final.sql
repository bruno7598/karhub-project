SELECT
  r.`ID Fonte Recurso`,
  r.`Nome Fonte Recurso`,
  CONCAT('R$', FORMAT('%13.2f', COALESCE(g.Total_Gasto, 0))) AS `Total Liquidado`,
  CONCAT('R$', FORMAT('%13.2f', COALESCE(a.Total_Arrecadado, 0))) AS `Total Arrecadado`
FROM (
  SELECT
    `ID Fonte Recurso`,
    `Nome Fonte Recurso`
  FROM
    `regal-state-348418.dataset_karhub.fonte_recursos_receita`
  UNION DISTINCT
  SELECT
    `ID Fonte Recurso`,
    `Nome Fonte Recurso`
  FROM
    `regal-state-348418.dataset_karhub.fonte_recursos_despesas`
) r
LEFT JOIN (
  SELECT
    `ID Fonte Recurso`,
    SUM(Liquidado) AS Total_Gasto
  FROM
    `regal-state-348418.dataset_karhub.fonte_recursos_despesas`
  GROUP BY
    `ID Fonte Recurso`
) g
ON
  r.`ID Fonte Recurso` = g.`ID Fonte Recurso`
LEFT JOIN (
  SELECT
    `ID Fonte Recurso`,
    SUM(Arrecadado) AS Total_Arrecadado
  FROM
    `regal-state-348418.dataset_karhub.fonte_recursos_receita`
  GROUP BY
    `ID Fonte Recurso`
) a
ON
  r.`ID Fonte Recurso` = a.`ID Fonte Recurso`
