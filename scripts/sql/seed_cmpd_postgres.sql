CREATE TABLE IF NOT EXISTS perfil_saude (
    numero_cpf VARCHAR(11) PRIMARY KEY,
    genero_identidade VARCHAR(16) NULL,
    fumante_status VARCHAR(24) NULL,
    imc_atual NUMERIC(5, 2) NULL
);

BEGIN;
TRUNCATE TABLE perfil_saude;

INSERT INTO perfil_saude (
    numero_cpf,
    genero_identidade,
    fumante_status,
    imc_atual
)
SELECT
    'DEV' || LPAD(n::text, 8, '0'),
    CASE
        WHEN n % 23 = 0 THEN NULL
        WHEN n % 2 = 0 THEN 'feminino'
        ELSE 'masculino'
    END,
    CASE
        WHEN n % 13 = 0 THEN NULL
        WHEN n % 3 = 0 THEN 'fumante atual'
        WHEN n % 3 = 1 THEN 'nunca fumou'
        ELSE 'ex-fumante'
    END,
    CASE
        WHEN n % 29 = 0 THEN NULL
        ELSE ROUND((18.00 + ((n * 37) % 1700) / 100.0)::numeric, 2)
    END
FROM generate_series(1, 1507) AS synthetic_rows(n);
COMMIT;
