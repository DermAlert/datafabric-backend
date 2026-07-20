-- =========================================================
-- db_telemedicina_setup_mysql.sql
-- Run after connecting to a MySQL 8+ database.
-- MySQL equivalent of db_telemedicina_setup.sql for Experiment 1.
-- =========================================================

DROP TEMPORARY TABLE IF EXISTS tmp_digits;
DROP TEMPORARY TABLE IF EXISTS seq_numbers;

DROP TABLE IF EXISTS prescricoes_consulta;
DROP TABLE IF EXISTS consultas;
DROP TABLE IF EXISTS profissionais;
DROP TABLE IF EXISTS pacientes_seed;

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

CREATE TABLE profissionais (
    profissional_id     BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    conselho_numero     VARCHAR(20) NOT NULL UNIQUE,
    professional_name   TEXT NOT NULL,
    specialty           VARCHAR(30) NOT NULL,
    state_code          CHAR(2) NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE INDEX idx_profissionais_specialty ON profissionais(specialty);

INSERT INTO profissionais (conselho_numero, professional_name, specialty, state_code)
SELECT
    CONCAT('CRM', LPAD(n, 7, '0')),
    CONCAT('profissional_', n),
    CASE
        WHEN MOD(n, 4) = 0 THEN 'dermatology'
        WHEN MOD(n, 4) = 1 THEN 'primary_care'
        WHEN MOD(n, 4) = 2 THEN 'oncology'
        ELSE 'general_practice'
    END,
    CASE
        WHEN MOD(n, 5) = 0 THEN 'SP'
        WHEN MOD(n, 5) = 1 THEN 'DF'
        WHEN MOD(n, 5) = 2 THEN 'RJ'
        WHEN MOD(n, 5) = 3 THEN 'MG'
        ELSE 'BA'
    END
FROM seq_numbers
WHERE n <= 180
ORDER BY n;

CREATE TABLE pacientes_seed (
    patient_id          BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    cpf                 VARCHAR(11) NOT NULL UNIQUE,
    patient_name        TEXT NOT NULL,
    sex                 CHAR(1) NOT NULL CHECK (sex IN ('M', 'F')),
    birth_date          DATE NOT NULL,
    municipality_code   INT NOT NULL,
    engagement_score    DECIMAL(5,2) NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE INDEX idx_pacientes_cpf ON pacientes_seed(cpf);

INSERT INTO pacientes_seed (cpf, patient_name, sex, birth_date, municipality_code, engagement_score)
SELECT
    LPAD(n, 11, '0'),
    CONCAT('patient_', n),
    CASE WHEN MOD(n, 2) = 0 THEN 'F' ELSE 'M' END,
    DATE_ADD('1958-01-01', INTERVAL MOD(n * 29, 20000) DAY),
    100000 + MOD(n, 40),
    ROUND(20 + MOD(n * 11, 70), 2)
FROM seq_numbers
ORDER BY n;

CREATE TABLE consultas (
    consultation_id     BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    cpf                 VARCHAR(11) NOT NULL,
    profissional_id     BIGINT NOT NULL,
    consultation_date   DATE NOT NULL,
    specialty           VARCHAR(30) NOT NULL,
    complaint           VARCHAR(30) NOT NULL,
    outcome_status      VARCHAR(30) NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_consultas_profissional
        FOREIGN KEY (profissional_id) REFERENCES profissionais(profissional_id)
) ENGINE=InnoDB;

CREATE INDEX idx_consultas_cpf ON consultas(cpf);
CREATE INDEX idx_consultas_date ON consultas(consultation_date);
CREATE INDEX idx_consultas_specialty ON consultas(specialty);

INSERT INTO consultas (
    cpf, profissional_id, consultation_date, specialty, complaint, outcome_status
)
SELECT
    p.cpf,
    1 + MOD(p.patient_id + runs.n, 180),
    DATE_ADD('2024-01-01', INTERVAL MOD((p.patient_id * 5) + (runs.n * 17), 540) DAY),
    CASE
        WHEN MOD(p.patient_id, 4) = 0 THEN 'dermatology'
        WHEN MOD(p.patient_id, 4) = 1 THEN 'primary_care'
        WHEN MOD(p.patient_id, 4) = 2 THEN 'oncology'
        ELSE 'general_practice'
    END,
    CASE
        WHEN MOD(p.patient_id, 6) = 0 THEN 'pigmented lesion'
        WHEN MOD(p.patient_id, 6) = 1 THEN 'rash'
        WHEN MOD(p.patient_id, 6) = 2 THEN 'follow_up'
        WHEN MOD(p.patient_id, 6) = 3 THEN 'screening'
        WHEN MOD(p.patient_id, 6) = 4 THEN 'biopsy review'
        ELSE 'teletriage'
    END,
    CASE
        WHEN MOD(p.patient_id + runs.n, 5) = 0 THEN 'referred'
        WHEN MOD(p.patient_id + runs.n, 5) = 1 THEN 'resolved'
        ELSE 'monitoring'
    END
FROM pacientes_seed AS p
JOIN (
    SELECT 1 AS n
    UNION ALL SELECT 2
) AS runs
    ON runs.n <= CASE
        WHEN MOD(p.patient_id, 10) < 6 THEN 1 + MOD(p.patient_id, 2)
        ELSE 0
    END
ORDER BY p.patient_id, runs.n;

CREATE TABLE prescricoes_consulta (
    prescription_id     BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    consultation_id     BIGINT NOT NULL,
    cpf                 VARCHAR(11) NOT NULL,
    medication_code     VARCHAR(20) NOT NULL,
    dosage_text         VARCHAR(30) NOT NULL,
    prescription_date   DATE NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_prescricoes_consulta
        FOREIGN KEY (consultation_id) REFERENCES consultas(consultation_id)
) ENGINE=InnoDB;

CREATE INDEX idx_prescricoes_consulta ON prescricoes_consulta(consultation_id);
CREATE INDEX idx_prescricoes_cpf ON prescricoes_consulta(cpf);
CREATE INDEX idx_prescricoes_date ON prescricoes_consulta(prescription_date);

INSERT INTO prescricoes_consulta (
    consultation_id, cpf, medication_code, dosage_text, prescription_date
)
SELECT
    c.consultation_id,
    c.cpf,
    CONCAT('MED_', LPAD(MOD(c.consultation_id, 90) + runs.n, 3, '0')),
    CASE
        WHEN MOD(c.consultation_id + runs.n, 3) = 0 THEN '1x/day'
        WHEN MOD(c.consultation_id + runs.n, 3) = 1 THEN '2x/day'
        ELSE 'as needed'
    END,
    DATE_ADD(c.consultation_date, INTERVAL MOD(runs.n - 1, 5) DAY)
FROM consultas AS c
JOIN (
    SELECT 1 AS n
    UNION ALL SELECT 2
) AS runs
    ON runs.n <= 1 + MOD(c.consultation_id, 2)
ORDER BY c.consultation_id, runs.n;

ANALYZE TABLE profissionais;
ANALYZE TABLE pacientes_seed;
ANALYZE TABLE consultas;
ANALYZE TABLE prescricoes_consulta;
