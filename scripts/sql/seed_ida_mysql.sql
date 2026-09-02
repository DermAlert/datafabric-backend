SET NAMES utf8mb4;

CREATE TABLE IF NOT EXISTS ficha_dermato (
    cpf_titular VARCHAR(11) NOT NULL PRIMARY KEY,
    sexo_biologico VARCHAR(16) NULL,
    cor_pele_fitzpatrick VARCHAR(40) NULL
);

SET SESSION cte_max_recursion_depth = 2000;

START TRANSACTION;
DELETE FROM ficha_dermato;

INSERT INTO ficha_dermato (
    cpf_titular,
    sexo_biologico,
    cor_pele_fitzpatrick
)
WITH RECURSIVE synthetic_rows (n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM synthetic_rows WHERE n < 1507
)
SELECT
    CONCAT('DEV', LPAD(n, 8, '0')),
    CASE
        WHEN MOD(n, 17) = 0 THEN NULL
        WHEN MOD(n, 2) = 0 THEN 'feminino'
        ELSE 'masculino'
    END,
    CASE
        WHEN MOD(n, 19) = 0 THEN NULL
        WHEN MOD(n, 6) = 0 THEN 'Tipo I - muito clara'
        WHEN MOD(n, 6) = 1 THEN 'Tipo II - clara'
        WHEN MOD(n, 6) = 2 THEN 'Tipo III - média'
        WHEN MOD(n, 6) = 3 THEN 'Tipo IV - morena clara'
        WHEN MOD(n, 6) = 4 THEN 'Tipo V - morena escura'
        ELSE 'Tipo VI - negra'
    END
FROM synthetic_rows;
COMMIT;
