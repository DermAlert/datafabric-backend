-- =========================================================
-- db_imagem_setup_mysql.sql
-- Run after connecting to a MySQL 8+ database.
-- MySQL equivalent of db_imagem_setup.sql for Experiment 1.
-- =========================================================

DROP TEMPORARY TABLE IF EXISTS tmp_digits;
DROP TEMPORARY TABLE IF EXISTS seq_numbers;

DROP TABLE IF EXISTS laudos_imagem;
DROP TABLE IF EXISTS capturas_imagem;
DROP TABLE IF EXISTS equipamentos_imagem;
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

CREATE TABLE equipamentos_imagem (
    equipamento_id      BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    equipment_code      VARCHAR(20) NOT NULL UNIQUE,
    modality            VARCHAR(30) NOT NULL,
    site_name           TEXT NOT NULL,
    municipality_code   INT NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE INDEX idx_equipamentos_modality ON equipamentos_imagem(modality);

INSERT INTO equipamentos_imagem (equipment_code, modality, site_name, municipality_code)
SELECT
    CONCAT('IMG', LPAD(n, 5, '0')),
    CASE
        WHEN MOD(n, 3) = 0 THEN 'dermoscopy'
        WHEN MOD(n, 3) = 1 THEN 'macroscopy'
        ELSE 'confocal'
    END,
    CONCAT('site_', n),
    100000 + MOD(n, 40)
FROM seq_numbers
WHERE n <= 110
ORDER BY n;

CREATE TABLE pacientes_seed (
    patient_id          BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    cpf                 VARCHAR(11) NOT NULL UNIQUE,
    patient_name        TEXT NOT NULL,
    sex                 CHAR(1) NOT NULL CHECK (sex IN ('M', 'F')),
    birth_date          DATE NOT NULL,
    municipality_code   INT NOT NULL,
    imaging_risk_score  DECIMAL(5,2) NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE INDEX idx_pacientes_cpf ON pacientes_seed(cpf);

INSERT INTO pacientes_seed (cpf, patient_name, sex, birth_date, municipality_code, imaging_risk_score)
SELECT
    LPAD(n, 11, '0'),
    CONCAT('patient_', n),
    CASE WHEN MOD(n, 2) = 0 THEN 'F' ELSE 'M' END,
    DATE_ADD('1958-01-01', INTERVAL MOD(n * 29, 20000) DAY),
    100000 + MOD(n, 40),
    ROUND(5 + MOD(n * 13, 95), 2)
FROM seq_numbers
ORDER BY n;

CREATE TABLE capturas_imagem (
    image_capture_id    BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    cpf                 VARCHAR(11) NOT NULL,
    equipamento_id      BIGINT NOT NULL,
    captured_at         DATE NOT NULL,
    body_site           VARCHAR(30) NOT NULL,
    suspicion_score     DECIMAL(5,2) NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_capturas_equipamento
        FOREIGN KEY (equipamento_id) REFERENCES equipamentos_imagem(equipamento_id)
) ENGINE=InnoDB;

CREATE INDEX idx_capturas_cpf ON capturas_imagem(cpf);
CREATE INDEX idx_capturas_date ON capturas_imagem(captured_at);
CREATE INDEX idx_capturas_score ON capturas_imagem(suspicion_score);
CREATE INDEX idx_capturas_body_site ON capturas_imagem(body_site);

INSERT INTO capturas_imagem (
    cpf, equipamento_id, captured_at, body_site, suspicion_score
)
SELECT
    p.cpf,
    1 + MOD(p.patient_id + runs.n, 110),
    DATE_ADD('2024-01-01', INTERVAL MOD((p.patient_id * 3) + (runs.n * 29), 540) DAY),
    CASE
        WHEN MOD(p.patient_id, 5) = 0 THEN 'face'
        WHEN MOD(p.patient_id, 5) = 1 THEN 'torso'
        WHEN MOD(p.patient_id, 5) = 2 THEN 'arm'
        WHEN MOD(p.patient_id, 5) = 3 THEN 'leg'
        ELSE 'scalp'
    END,
    ROUND(0.10 + (MOD((p.patient_id * 3) + (runs.n * 7), 90) / 100.0), 2)
FROM pacientes_seed AS p
JOIN (
    SELECT 1 AS n
    UNION ALL SELECT 2
) AS runs
    ON runs.n <= CASE
        WHEN MOD(p.patient_id, 10) < 5 THEN 1 + MOD(p.patient_id, 2)
        ELSE 0
    END
ORDER BY p.patient_id, runs.n;

CREATE TABLE laudos_imagem (
    report_id           BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    image_capture_id    BIGINT NOT NULL,
    cpf                 VARCHAR(11) NOT NULL,
    lesion_pattern      VARCHAR(30) NOT NULL,
    report_status       VARCHAR(30) NOT NULL,
    signed_at           DATE NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_laudos_capture
        FOREIGN KEY (image_capture_id) REFERENCES capturas_imagem(image_capture_id)
) ENGINE=InnoDB;

CREATE INDEX idx_laudos_capture ON laudos_imagem(image_capture_id);
CREATE INDEX idx_laudos_cpf ON laudos_imagem(cpf);
CREATE INDEX idx_laudos_status ON laudos_imagem(report_status);

INSERT INTO laudos_imagem (
    image_capture_id, cpf, lesion_pattern, report_status, signed_at
)
SELECT
    c.image_capture_id,
    c.cpf,
    CASE
        WHEN MOD(c.image_capture_id + 1, 4) = 0 THEN 'asymmetric'
        WHEN MOD(c.image_capture_id + 1, 4) = 1 THEN 'symmetric'
        WHEN MOD(c.image_capture_id + 1, 4) = 2 THEN 'multicomponent'
        ELSE 'homogeneous'
    END,
    CASE
        WHEN MOD(c.image_capture_id + 1, 5) = 0 THEN 'review_required'
        WHEN MOD(c.image_capture_id + 1, 5) = 1 THEN 'critical'
        ELSE 'final'
    END,
    c.captured_at
FROM capturas_imagem AS c
ORDER BY c.image_capture_id;

ANALYZE TABLE equipamentos_imagem;
ANALYZE TABLE pacientes_seed;
ANALYZE TABLE capturas_imagem;
ANALYZE TABLE laudos_imagem;
