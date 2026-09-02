SELECT VERSION() AS mysql_version;

SELECT COUNT(*) AS user_table_count
FROM information_schema.tables
WHERE table_schema = 'ida'
  AND table_type = 'BASE TABLE';

SELECT COUNT(*) AS total_columns
FROM information_schema.columns
WHERE table_schema = 'ida'
  AND table_name = 'ficha_dermato';

SELECT
    COUNT(*) AS total_rows,
    SUM(cpf_titular IS NULL) AS cpf_titular_missing,
    ROUND(100.0 * SUM(cpf_titular IS NULL) / COUNT(*), 2) AS cpf_titular_missing_pct,
    SUM(sexo_biologico IS NULL) AS sexo_biologico_missing,
    ROUND(100.0 * SUM(sexo_biologico IS NULL) / COUNT(*), 2) AS sexo_biologico_missing_pct,
    SUM(cor_pele_fitzpatrick IS NULL) AS cor_pele_fitzpatrick_missing,
    ROUND(100.0 * SUM(cor_pele_fitzpatrick IS NULL) / COUNT(*), 2) AS cor_pele_fitzpatrick_missing_pct
FROM ficha_dermato;

SELECT sexo_biologico, COUNT(*) AS records
FROM ficha_dermato
GROUP BY sexo_biologico
ORDER BY sexo_biologico;

SELECT COUNT(*) AS exact_accented_values_before_silver
FROM ficha_dermato
WHERE BINARY cor_pele_fitzpatrick = BINARY 'Tipo III - média';
