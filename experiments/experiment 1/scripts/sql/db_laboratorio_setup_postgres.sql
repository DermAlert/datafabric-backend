-- =========================================================
-- db_laboratorio_setup_postgres.sql
-- Run after connecting to a PostgreSQL database.
-- PostgreSQL copy for remote Experiment 1 seeding.
-- =========================================================

SET statement_timeout = 0;

DROP TABLE IF EXISTS resultados_exame;
DROP TABLE IF EXISTS exames_solicitados;
DROP TABLE IF EXISTS laboratorios;
DROP TABLE IF EXISTS pacientes_seed;

CREATE TABLE laboratorios (
    laboratorio_id      BIGSERIAL PRIMARY KEY,
    lab_code            VARCHAR(20) NOT NULL UNIQUE,
    lab_name            TEXT NOT NULL,
    municipality_code   INTEGER NOT NULL,
    accreditation_level TEXT NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_laboratorios_municipality ON laboratorios(municipality_code);

INSERT INTO laboratorios (lab_code, lab_name, municipality_code, accreditation_level)
SELECT
    'LAB' || LPAD(gs::TEXT, 5, '0'),
    'laboratorio_' || gs,
    100000 + (gs % 40),
    CASE
        WHEN gs % 3 = 0 THEN 'gold'
        WHEN gs % 3 = 1 THEN 'silver'
        ELSE 'standard'
    END
FROM generate_series(1, 90) AS gs;

CREATE TABLE pacientes_seed (
    patient_id          BIGSERIAL PRIMARY KEY,
    cpf                 VARCHAR(11) NOT NULL UNIQUE,
    patient_name        TEXT NOT NULL,
    sex                 CHAR(1) NOT NULL CHECK (sex IN ('M', 'F')),
    birth_date          DATE NOT NULL,
    municipality_code   INTEGER NOT NULL,
    lab_risk_score      NUMERIC(5,2) NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_pacientes_cpf ON pacientes_seed(cpf);

INSERT INTO pacientes_seed (cpf, patient_name, sex, birth_date, municipality_code, lab_risk_score)
SELECT
    LPAD(gs::TEXT, 11, '0'),
    'patient_' || gs,
    CASE WHEN gs % 2 = 0 THEN 'F' ELSE 'M' END,
    DATE '1958-01-01' + (((gs * 29) % 20000)::int),
    100000 + (gs % 40),
    ROUND((10 + ((gs * 23) % 90))::NUMERIC, 2)
FROM generate_series(1, 10000) AS gs;

CREATE TABLE exames_solicitados (
    exam_request_id     BIGSERIAL PRIMARY KEY,
    cpf                 VARCHAR(11) NOT NULL,
    laboratorio_id      BIGINT NOT NULL REFERENCES laboratorios(laboratorio_id),
    exam_date           DATE NOT NULL,
    exam_type           TEXT NOT NULL,
    request_origin      TEXT NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_exames_cpf ON exames_solicitados(cpf);
CREATE INDEX idx_exames_date ON exames_solicitados(exam_date);
CREATE INDEX idx_exames_type ON exames_solicitados(exam_type);

INSERT INTO exames_solicitados (
    cpf, laboratorio_id, exam_date, exam_type, request_origin
)
SELECT
    p.cpf,
    (1 + ((p.patient_id + g.n) % 90))::bigint,
    DATE '2024-01-01' + ((((p.patient_id * 7) + (g.n * 19)) % 540)::int),
    CASE
        WHEN g.n % 4 = 0 THEN 'histopathology'
        WHEN g.n % 4 = 1 THEN 'cbc'
        WHEN g.n % 4 = 2 THEN 'biochemistry'
        ELSE 'immunology'
    END,
    CASE
        WHEN p.patient_id % 3 = 0 THEN 'sus'
        WHEN p.patient_id % 3 = 1 THEN 'telemedicine'
        ELSE 'private'
    END
FROM pacientes_seed p
JOIN LATERAL generate_series(
    1,
    CASE WHEN p.patient_id % 10 < 7 THEN 1 + (p.patient_id % 3) ELSE 0 END
) AS g(n) ON TRUE;

CREATE TABLE resultados_exame (
    exam_result_id      BIGSERIAL PRIMARY KEY,
    exam_request_id     BIGINT NOT NULL REFERENCES exames_solicitados(exam_request_id),
    cpf                 VARCHAR(11) NOT NULL,
    analyte_name        TEXT NOT NULL,
    result_value        NUMERIC(10,2),
    result_flag         TEXT NOT NULL,
    released_at         DATE NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_resultados_request ON resultados_exame(exam_request_id);
CREATE INDEX idx_resultados_cpf ON resultados_exame(cpf);
CREATE INDEX idx_resultados_flag ON resultados_exame(result_flag);
CREATE INDEX idx_resultados_date ON resultados_exame(released_at);

INSERT INTO resultados_exame (
    exam_request_id, cpf, analyte_name, result_value, result_flag, released_at
)
SELECT
    e.exam_request_id,
    e.cpf,
    CASE
        WHEN (e.exam_request_id + g.n) % 4 = 0 THEN 'marker_a'
        WHEN (e.exam_request_id + g.n) % 4 = 1 THEN 'marker_b'
        WHEN (e.exam_request_id + g.n) % 4 = 2 THEN 'marker_c'
        ELSE 'marker_d'
    END,
    ROUND((10 + ((e.exam_request_id * g.n) % 800) / 10.0)::NUMERIC, 2),
    CASE
        WHEN (e.exam_request_id + g.n) % 10 < 2 THEN 'critical'
        WHEN (e.exam_request_id + g.n) % 10 < 5 THEN 'high'
        ELSE 'normal'
    END,
    e.exam_date + (((g.n - 1) % 4)::int)
FROM exames_solicitados e
JOIN LATERAL generate_series(1, 1 + ((e.exam_request_id % 2)::int)) AS g(n) ON TRUE;

ANALYZE laboratorios;
ANALYZE pacientes_seed;
ANALYZE exames_solicitados;
ANALYZE resultados_exame;
