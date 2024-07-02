CREATE OR REPLACE VIEW `regal-state-348418.dataset_karhub.top_5_fontes_arrecadacao` AS
SELECT
  `ID Fonte Recurso`,
  `Nome Fonte Recurso`,
  CONCAT('R$', FORMAT('%13.2f', SUM(Arrecadado))) AS Total_Arrecadado
FROM
  `regal-state-348418.dataset_karhub.fonte_recursos_receita`
GROUP BY
  `ID Fonte Recurso`, `Nome Fonte Recurso`
ORDER BY
  SUM(Arrecadado) DESC
LIMIT 5;

CREATE OR REPLACE VIEW `regal-state-348418.dataset_karhub.top_5_fontes_gastos` AS
SELECT
  `ID Fonte Recurso`,
  `Nome Fonte Recurso`,
  CONCAT('R$', FORMAT('%13.2f', SUM(Liquidado))) AS Total_Gasto
FROM
  `regal-state-348418.dataset_karhub.fonte_recursos_despesas`
GROUP BY
  `ID Fonte Recurso`, `Nome Fonte Recurso`
ORDER BY
  SUM(Liquidado) DESC
LIMIT 5;

CREATE OR REPLACE VIEW `regal-state-348418.dataset_karhub.top_5_fontes_margem_bruta` AS
SELECT
  r.`ID Fonte Recurso`,
  r.`Nome Fonte Recurso`,
  CONCAT('R$', FORMAT('%13.2f', (SUM(r.Arrecadado) - COALESCE(SUM(d.Liquidado), 0)))) AS Margem_Bruta
FROM
  `regal-state-348418.dataset_karhub.fonte_recursos_receita` r
LEFT JOIN
  `regal-state-348418.dataset_karhub.fonte_recursos_despesas` d
ON
  r.`ID Fonte Recurso` = d.`ID Fonte Recurso`
GROUP BY
  r.`ID Fonte Recurso`, r.`Nome Fonte Recurso`
ORDER BY
  Margem_Bruta DESC
LIMIT 5;

CREATE OR REPLACE VIEW `regal-state-348418.dataset_karhub.top_5_fontes_menor_arrecadacao` AS
SELECT
  `ID Fonte Recurso`,
  `Nome Fonte Recurso`,
  CONCAT('R$', FORMAT('%13.2f', SUM(Arrecadado))) AS Total_Arrecadado
FROM
  `regal-state-348418.dataset_karhub.fonte_recursos_receita`
GROUP BY
  `ID Fonte Recurso`, `Nome Fonte Recurso`
ORDER BY
  SUM(Arrecadado) ASC
LIMIT 5;

CREATE OR REPLACE VIEW `regal-state-348418.dataset_karhub.top_5_fontes_menor_gastos` AS
SELECT
  `ID Fonte Recurso`,
  `Nome Fonte Recurso`,
  CONCAT('R$', FORMAT('%13.2f', SUM(Liquidado))) AS Total_Gasto
FROM
  `regal-state-348418.dataset_karhub.fonte_recursos_despesas`
GROUP BY
  `ID Fonte Recurso`, `Nome Fonte Recurso`
ORDER BY
  SUM(Liquidado) ASC
LIMIT 5;

CREATE OR REPLACE VIEW `regal-state-348418.dataset_karhub.top_5_fontes_pior_margem_bruta` AS
SELECT
  r.`ID Fonte Recurso`,
  r.`Nome Fonte Recurso`,
  CONCAT('R$', FORMAT('%13.2f', (SUM(r.Arrecadado) - COALESCE(SUM(d.Liquidado), 0)))) AS Margem_Bruta
FROM
  `regal-state-348418.dataset_karhub.fonte_recursos_receita` r
LEFT JOIN
  `regal-state-348418.dataset_karhub.fonte_recursos_despesas` d
ON
  r.`ID Fonte Recurso` = d.`ID Fonte Recurso`
GROUP BY
  r.`ID Fonte Recurso`, r.`Nome Fonte Recurso`
ORDER BY
  Margem_Bruta ASC
LIMIT 5;

CREATE OR REPLACE VIEW `regal-state-348418.dataset_karhub.media_arrecadacao_por_fonte` AS
SELECT
  `ID Fonte Recurso`,
  `Nome Fonte Recurso`,
  CONCAT('R$', FORMAT('%13.2f', AVG(Arrecadado))) AS Media_Arrecadacao
FROM
  `regal-state-348418.dataset_karhub.fonte_recursos_receita`
GROUP BY
  `ID Fonte Recurso`, `Nome Fonte Recurso`;

CREATE OR REPLACE VIEW `regal-state-348418.dataset_karhub.media_gastos_por_fonte` AS
SELECT
  `ID Fonte Recurso`,
  `Nome Fonte Recurso`,
  CONCAT('R$', FORMAT('%13.2f', AVG(Liquidado))) AS Media_Gastos
FROM
  `regal-state-348418.dataset_karhub.fonte_recursos_despesas`
GROUP BY
  `ID Fonte Recurso`, `Nome Fonte Recurso`;
