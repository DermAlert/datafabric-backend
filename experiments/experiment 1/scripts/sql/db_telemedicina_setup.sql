-- =========================================================
-- db_telemedicina_setup.sql
-- Run after connecting to database: db_telemedicina
-- =========================================================

SET statement_timeout = 0;

DROP TABLE IF EXISTS prescricoes_consulta;
DROP TABLE IF EXISTS consultas;
DROP TABLE IF EXISTS profissionais;
DROP TABLE IF EXISTS pacientes_seed;

CREATE TABLE profissionais (
    profissional_id     BIGSERIAL PRIMARY KEY,
    conselho_numero     VARCHAR(20) NOT NULL UNIQUE,
    professional_name   TEXT NOT NULL,
    specialty           TEXT NOT NULL,
    state_code          CHAR(2) NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_profissionais_specialty ON profissionais(specialty);

INSERT INTO profissionais (conselho_numero, professional_name, specialty, state_code)
SELECT
    'CRM' || LPAD(gs::TEXT, 7, '0'),
    'profissional_' || gs,
    CASE
        WHEN gs % 4 = 0 THEN 'dermatology'
        WHEN gs % 4 = 1 THEN 'primary_care'
        WHEN gs % 4 = 2 THEN 'oncology'
        ELSE 'general_practice'
    END,
    CASE
        WHEN gs % 5 = 0 THEN 'SP'
        WHEN gs % 5 = 1 THEN 'DF'
        WHEN gs % 5 = 2 THEN 'RJ'
        WHEN gs % 5 = 3 THEN 'MG'
        ELSE 'BA'
    END
FROM generate_series(1, 180) AS gs;

CREATE TABLE pacientes_seed (
    patient_id          BIGSERIAL PRIMARY KEY,
    cpf                 VARCHAR(11) NOT NULL UNIQUE,
    patient_name        TEXT NOT NULL,
    sex                 CHAR(1) NOT NULL CHECK (sex IN ('M', 'F')),
    birth_date          DATE NOT NULL,
    municipality_code   INTEGER NOT NULL,
    engagement_score    NUMERIC(5,2) NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_pacientes_cpf ON pacientes_seed(cpf);

INSERT INTO pacientes_seed (cpf, patient_name, sex, birth_date, municipality_code, engagement_score)
SELECT
    LPAD(gs::TEXT, 11, '0'),
    'patient_' || gs,
    CASE WHEN gs % 2 = 0 THEN 'F' ELSE 'M' END,
    DATE '1958-01-01' + (((gs * 29) % 20000)::int),
    100000 + (gs % 40),
    ROUND((20 + ((gs * 11) % 70))::NUMERIC, 2)
FROM generate_series(1, 10000) AS gs;

CREATE TABLE consultas (
    consultation_id     BIGSERIAL PRIMARY KEY,
    cpf                 VARCHAR(11) NOT NULL,
    profissional_id     BIGINT NOT NULL REFERENCES profissionais(profissional_id),
    consultation_date   DATE NOT NULL,
    specialty           TEXT NOT NULL,
    complaint           TEXT NOT NULL,
    outcome_status      TEXT NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_consultas_cpf ON consultas(cpf);
CREATE INDEX idx_consultas_date ON consultas(consultation_date);
CREATE INDEX idx_consultas_specialty ON consultas(specialty);

INSERT INTO consultas (
    cpf, profissional_id, consultation_date, specialty, complaint, outcome_status
)
SELECT
    p.cpf,
    (1 + ((p.patient_id + g.n) % 180))::bigint,
    DATE '2024-01-01' + ((((p.patient_id * 5) + (g.n * 17)) % 540)::int),
    CASE
        WHEN p.patient_id % 4 = 0 THEN 'dermatology'
        WHEN p.patient_id % 4 = 1 THEN 'primary_care'
        WHEN p.patient_id % 4 = 2 THEN 'oncology'
        ELSE 'general_practice'
    END,
    CASE
        WHEN p.patient_id % 6 = 0 THEN 'pigmented lesion'
        WHEN p.patient_id % 6 = 1 THEN 'rash'
        WHEN p.patient_id % 6 = 2 THEN 'follow_up'
        WHEN p.patient_id % 6 = 3 THEN 'screening'
        WHEN p.patient_id % 6 = 4 THEN 'biopsy review'
        ELSE 'teletriage'
    END,
    CASE
        WHEN (p.patient_id + g.n) % 5 = 0 THEN 'referred'
        WHEN (p.patient_id + g.n) % 5 = 1 THEN 'resolved'
        ELSE 'monitoring'
    END
FROM pacientes_seed p
JOIN LATERAL generate_series(
    1,
    CASE WHEN p.patient_id % 10 < 6 THEN 1 + (p.patient_id % 2) ELSE 0 END
) AS g(n) ON TRUE;

CREATE TABLE prescricoes_consulta (
    prescription_id     BIGSERIAL PRIMARY KEY,
    consultation_id     BIGINT NOT NULL REFERENCES consultas(consultation_id),
    cpf                 VARCHAR(11) NOT NULL,
    medication_code     VARCHAR(20) NOT NULL,
    dosage_text         TEXT NOT NULL,
    prescription_date   DATE NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_prescricoes_consulta ON prescricoes_consulta(consultation_id);
CREATE INDEX idx_prescricoes_cpf ON prescricoes_consulta(cpf);
CREATE INDEX idx_prescricoes_date ON prescricoes_consulta(prescription_date);

INSERT INTO prescricoes_consulta (
    consultation_id, cpf, medication_code, dosage_text, prescription_date
)
SELECT
    c.consultation_id,
    c.cpf,
    'MED_' || LPAD((((c.consultation_id % 90) + g.n))::TEXT, 3, '0'),
    CASE
        WHEN (c.consultation_id + g.n) % 3 = 0 THEN '1x/day'
        WHEN (c.consultation_id + g.n) % 3 = 1 THEN '2x/day'
        ELSE 'as needed'
    END,
    c.consultation_date + (((g.n - 1) % 5)::int)
FROM consultas c
JOIN LATERAL generate_series(1, 1 + ((c.consultation_id % 2)::int)) AS g(n) ON TRUE;

ANALYZE profissionais;
ANALYZE pacientes_seed;
ANALYZE consultas;
ANALYZE prescricoes_consulta;
