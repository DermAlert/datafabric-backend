-- =========================================================
-- db_sus_setup_mysql.sql
-- Run after connecting to a MySQL 8+ database.
-- MySQL equivalent of db_sus_setup_10k.sql for Experiment 1.
-- =========================================================

DROP TEMPORARY TABLE IF EXISTS tmp_digits;
DROP TEMPORARY TABLE IF EXISTS seq_numbers;

DROP TABLE IF EXISTS procedimentos_atendimento;
DROP TABLE IF EXISTS atendimentos;
DROP TABLE IF EXISTS pacientes_seed;
DROP TABLE IF EXISTS unidades_saude;

CREATE TEMPORARY TABLE tmp_digits (
    d INT PRIMARY KEY
);

INSERT INTO tmp_digits (d)
VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);

CREATE TEMPORARY TABLE seq_numbers (
    n INT PRIMARY KEY
);

INSERT INTO seq_numbers (n)
SELECT number_rows.n
FROM (
    SELECT
        ones.d
        + (tens.d * 10)
        + (hundreds.d * 100)
        + (thousands.d * 1000)
        + 1 AS n
    FROM (
        SELECT 0 AS d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
    ) AS ones
    CROSS JOIN (
        SELECT 0 AS d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
    ) AS tens
    CROSS JOIN (
        SELECT 0 AS d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
    ) AS hundreds
    CROSS JOIN (
        SELECT 0 AS d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
    ) AS thousands
) AS number_rows
WHERE number_rows.n <= 10000
ORDER BY number_rows.n;

CREATE TABLE unidades_saude (
    unidade_id         BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    cnes_code          VARCHAR(10) NOT NULL UNIQUE,
    unit_name          TEXT NOT NULL,
    municipality_code  INT NOT NULL,
    unit_type          VARCHAR(30) NOT NULL,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE INDEX idx_unidades_municipality ON unidades_saude(municipality_code);

INSERT INTO unidades_saude (cnes_code, unit_name, municipality_code, unit_type)
SELECT
    CONCAT('CNES', LPAD(n, 6, '0')),
    CONCAT('unidade_', n),
    100000 + MOD(n, 40),
    CASE
        WHEN MOD(n, 4) = 0 THEN 'UBS'
        WHEN MOD(n, 4) = 1 THEN 'UPA'
        WHEN MOD(n, 4) = 2 THEN 'Hospital'
        ELSE 'Ambulatorio'
    END
FROM seq_numbers
WHERE n <= 120
ORDER BY n;

CREATE TABLE pacientes_seed (
    patient_id          BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    cpf                 VARCHAR(11) NOT NULL UNIQUE,
    patient_name        TEXT NOT NULL,
    sex                 CHAR(1) NOT NULL CHECK (sex IN ('M', 'F')),
    birth_date          DATE NOT NULL,
    municipality_code   INT NOT NULL,
    risk_score          DECIMAL(5,2) NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE INDEX idx_pacientes_cpf ON pacientes_seed(cpf);
CREATE INDEX idx_pacientes_municipality ON pacientes_seed(municipality_code);

INSERT INTO pacientes_seed (cpf, patient_name, sex, birth_date, municipality_code, risk_score)
SELECT
    LPAD(n, 11, '0'),
    CONCAT('patient_', n),
    CASE WHEN MOD(n, 2) = 0 THEN 'F' ELSE 'M' END,
    DATE_ADD('1958-01-01', INTERVAL MOD(n * 29, 20000) DAY),
    100000 + MOD(n, 40),
    ROUND(15 + MOD(n * 17, 85), 2)
FROM seq_numbers
ORDER BY n;

CREATE TABLE atendimentos (
    encounter_id        BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    cpf                 VARCHAR(11) NOT NULL,
    unidade_id          BIGINT NOT NULL,
    encounter_date      DATE NOT NULL,
    cid10_code          VARCHAR(10) NOT NULL,
    priority_level      TINYINT NOT NULL CHECK (priority_level BETWEEN 1 AND 5),
    outcome_status      VARCHAR(30) NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_atendimentos_unidade
        FOREIGN KEY (unidade_id) REFERENCES unidades_saude(unidade_id)
) ENGINE=InnoDB;

CREATE INDEX idx_atendimentos_cpf ON atendimentos(cpf);
CREATE INDEX idx_atendimentos_date ON atendimentos(encounter_date);
CREATE INDEX idx_atendimentos_cid10 ON atendimentos(cid10_code);
CREATE INDEX idx_atendimentos_unidade ON atendimentos(unidade_id);

INSERT INTO atendimentos (
    cpf, unidade_id, encounter_date, cid10_code, priority_level, outcome_status
)
SELECT
    p.cpf,
    1 + MOD(p.patient_id + runs.n, 120),
    DATE_ADD('2024-01-01', INTERVAL MOD((p.patient_id * 9) + (runs.n * 13), 540) DAY),
    CASE
        WHEN MOD(p.patient_id, 9) = 0 THEN 'C43'
        WHEN MOD(p.patient_id, 7) = 0 THEN 'D22'
        WHEN MOD(p.patient_id, 5) = 0 THEN 'L57'
        WHEN MOD(p.patient_id, 3) = 0 THEN 'Z12'
        ELSE 'R21'
    END,
    1 + MOD(p.patient_id + runs.n, 5),
    CASE
        WHEN MOD(p.patient_id + runs.n, 4) = 0 THEN 'referred'
        WHEN MOD(p.patient_id + runs.n, 4) = 1 THEN 'resolved'
        ELSE 'monitoring'
    END
FROM pacientes_seed AS p
JOIN (
    SELECT 1 AS n
    UNION ALL SELECT 2
    UNION ALL SELECT 3
) AS runs
    ON runs.n <= CASE
        WHEN MOD(p.patient_id, 10) < 8 THEN 1 + MOD(p.patient_id, 3)
        ELSE 0
    END
ORDER BY p.patient_id, runs.n;

CREATE TABLE procedimentos_atendimento (
    procedure_id        BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    encounter_id        BIGINT NOT NULL,
    cpf                 VARCHAR(11) NOT NULL,
    procedure_code      VARCHAR(20) NOT NULL,
    procedure_group     VARCHAR(30) NOT NULL,
    procedure_date      DATE NOT NULL,
    approved_flag       BOOLEAN NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_procedimentos_encounter
        FOREIGN KEY (encounter_id) REFERENCES atendimentos(encounter_id)
) ENGINE=InnoDB;

CREATE INDEX idx_procedimentos_encounter ON procedimentos_atendimento(encounter_id);
CREATE INDEX idx_procedimentos_cpf ON procedimentos_atendimento(cpf);
CREATE INDEX idx_procedimentos_date ON procedimentos_atendimento(procedure_date);
CREATE INDEX idx_procedimentos_group ON procedimentos_atendimento(procedure_group);

INSERT INTO procedimentos_atendimento (
    encounter_id, cpf, procedure_code, procedure_group, procedure_date, approved_flag
)
SELECT
    a.encounter_id,
    a.cpf,
    CONCAT('PROC_', LPAD(MOD(a.encounter_id, 180) + runs.n, 4, '0')),
    CASE
        WHEN MOD(a.encounter_id + runs.n, 4) = 0 THEN 'consulta'
        WHEN MOD(a.encounter_id + runs.n, 4) = 1 THEN 'triagem'
        WHEN MOD(a.encounter_id + runs.n, 4) = 2 THEN 'biopsia'
        ELSE 'retorno'
    END,
    DATE_ADD(a.encounter_date, INTERVAL MOD(runs.n - 1, 7) DAY),
    CASE WHEN MOD(a.encounter_id + runs.n, 5) <> 0 THEN TRUE ELSE FALSE END
FROM atendimentos AS a
JOIN (
    SELECT 1 AS n
    UNION ALL SELECT 2
) AS runs
    ON runs.n <= 1 + MOD(a.encounter_id, 2)
ORDER BY a.encounter_id, runs.n;

ANALYZE TABLE unidades_saude;
ANALYZE TABLE pacientes_seed;
ANALYZE TABLE atendimentos;
ANALYZE TABLE procedimentos_atendimento;
