SELECT version() AS postgres_version;

SELECT COUNT(*) AS user_table_count
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE';

SELECT COUNT(*) AS total_columns
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'perfil_saude';

SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE numero_cpf IS NULL) AS numero_cpf_missing,
    ROUND(100.0 * COUNT(*) FILTER (WHERE numero_cpf IS NULL) / COUNT(*), 2) AS numero_cpf_missing_pct,
    COUNT(*) FILTER (WHERE genero_identidade IS NULL) AS genero_identidade_missing,
    ROUND(100.0 * COUNT(*) FILTER (WHERE genero_identidade IS NULL) / COUNT(*), 2) AS genero_identidade_missing_pct,
    COUNT(*) FILTER (WHERE fumante_status IS NULL) AS fumante_status_missing,
    ROUND(100.0 * COUNT(*) FILTER (WHERE fumante_status IS NULL) / COUNT(*), 2) AS fumante_status_missing_pct,
    COUNT(*) FILTER (WHERE imc_atual IS NULL) AS imc_atual_missing,
    ROUND(100.0 * COUNT(*) FILTER (WHERE imc_atual IS NULL) / COUNT(*), 2) AS imc_atual_missing_pct
FROM perfil_saude;

SELECT COALESCE(genero_identidade, '<NULL>') AS genero_identidade, COUNT(*) AS records
FROM perfil_saude
GROUP BY genero_identidade
ORDER BY genero_identidade NULLS FIRST;

SELECT COUNT(*) AS lowercase_values_before_silver
FROM perfil_saude
WHERE fumante_status IS NOT NULL
  AND fumante_status <> upper(fumante_status);
