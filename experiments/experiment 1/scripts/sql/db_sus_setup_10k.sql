-- =========================================================
-- db_sus_setup.sql
-- Run after connecting to database: db_sus
-- Deletes old tables, recreates them, and populates with
-- synthetic data for Experiment 1.
-- =========================================================

SET statement_timeout = 0;

DROP TABLE IF EXISTS procedimentos_atendimento;
DROP TABLE IF EXISTS atendimentos;
DROP TABLE IF EXISTS pacientes_seed;
DROP TABLE IF EXISTS unidades_saude;

CREATE TABLE unidades_saude (
    unidade_id         BIGSERIAL PRIMARY KEY,
    cnes_code          VARCHAR(10) NOT NULL UNIQUE,
    unit_name          TEXT NOT NULL,
    municipality_code  INTEGER NOT NULL,
    unit_type          TEXT NOT NULL,
    created_at         TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_unidades_municipality ON unidades_saude(municipality_code);

INSERT INTO unidades_saude (cnes_code, unit_name, municipality_code, unit_type)
SELECT
    'CNES' || LPAD(gs::TEXT, 6, '0'),
    'unidade_' || gs,
    100000 + (gs % 40),
    CASE
        WHEN gs % 4 = 0 THEN 'UBS'
        WHEN gs % 4 = 1 THEN 'UPA'
        WHEN gs % 4 = 2 THEN 'Hospital'
        ELSE 'Ambulatorio'
    END
FROM generate_series(1, 120) AS gs;

CREATE TABLE pacientes_seed (
    patient_id          BIGSERIAL PRIMARY KEY,
    cpf                 VARCHAR(11) NOT NULL UNIQUE,
    patient_name        TEXT NOT NULL,
    sex                 CHAR(1) NOT NULL CHECK (sex IN ('M', 'F')),
    birth_date          DATE NOT NULL,
    municipality_code   INTEGER NOT NULL,
    risk_score          NUMERIC(5,2) NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_pacientes_cpf ON pacientes_seed(cpf);
CREATE INDEX idx_pacientes_municipality ON pacientes_seed(municipality_code);

INSERT INTO pacientes_seed (cpf, patient_name, sex, birth_date, municipality_code, risk_score)
SELECT
    LPAD(gs::TEXT, 11, '0'),
    'patient_' || gs,
    CASE WHEN gs % 2 = 0 THEN 'F' ELSE 'M' END,
    DATE '1958-01-01' + (((gs * 29) % 20000)::int),
    100000 + (gs % 40),
    ROUND((15 + ((gs * 17) % 85))::NUMERIC, 2)
FROM generate_series(1, 10000) AS gs;

CREATE TABLE atendimentos (
    encounter_id        BIGSERIAL PRIMARY KEY,
    cpf                 VARCHAR(11) NOT NULL,
    unidade_id          BIGINT NOT NULL REFERENCES unidades_saude(unidade_id),
    encounter_date      DATE NOT NULL,
    cid10_code          VARCHAR(10) NOT NULL,
    priority_level      SMALLINT NOT NULL CHECK (priority_level BETWEEN 1 AND 5),
    outcome_status      TEXT NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_atendimentos_cpf ON atendimentos(cpf);
CREATE INDEX idx_atendimentos_date ON atendimentos(encounter_date);
CREATE INDEX idx_atendimentos_cid10 ON atendimentos(cid10_code);
CREATE INDEX idx_atendimentos_unidade ON atendimentos(unidade_id);

INSERT INTO atendimentos (cpf, unidade_id, encounter_date, cid10_code, priority_level, outcome_status)
SELECT
    p.cpf,
    (1 + ((p.patient_id + g.n) % 120))::bigint,
    DATE '2024-01-01' + ((((p.patient_id * 9) + (g.n * 13)) % 540)::int),
    CASE
        WHEN p.patient_id % 9 = 0 THEN 'C43'
        WHEN p.patient_id % 7 = 0 THEN 'D22'
        WHEN p.patient_id % 5 = 0 THEN 'L57'
        WHEN p.patient_id % 3 = 0 THEN 'Z12'
        ELSE 'R21'
    END,
    (1 + ((p.patient_id + g.n) % 5))::smallint,
    CASE
        WHEN (p.patient_id + g.n) % 4 = 0 THEN 'referred'
        WHEN (p.patient_id + g.n) % 4 = 1 THEN 'resolved'
        ELSE 'monitoring'
    END
FROM pacientes_seed p
JOIN LATERAL generate_series(
    1,
    CASE
        WHEN p.patient_id % 10 < 8 THEN 1 + (p.patient_id % 3)
        ELSE 0
    END
) AS g(n) ON TRUE;

CREATE TABLE procedimentos_atendimento (
    procedure_id        BIGSERIAL PRIMARY KEY,
    encounter_id        BIGINT NOT NULL REFERENCES atendimentos(encounter_id),
    cpf                 VARCHAR(11) NOT NULL,
    procedure_code      VARCHAR(20) NOT NULL,
    procedure_group     TEXT NOT NULL,
    procedure_date      DATE NOT NULL,
    approved_flag       BOOLEAN NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

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
    'PROC_' || LPAD((((a.encounter_id % 180) + g.n))::TEXT, 4, '0'),
    CASE
        WHEN (a.encounter_id + g.n) % 4 = 0 THEN 'consulta'
        WHEN (a.encounter_id + g.n) % 4 = 1 THEN 'triagem'
        WHEN (a.encounter_id + g.n) % 4 = 2 THEN 'biopsia'
        ELSE 'retorno'
    END,
    a.encounter_date + (((g.n - 1) % 7)::int),
    CASE WHEN (a.encounter_id + g.n) % 5 <> 0 THEN TRUE ELSE FALSE END
FROM atendimentos a
JOIN LATERAL generate_series(1, 1 + ((a.encounter_id % 2)::int)) AS g(n) ON TRUE;

ANALYZE unidades_saude;
ANALYZE pacientes_seed;
ANALYZE atendimentos;
ANALYZE procedimentos_atendimento;