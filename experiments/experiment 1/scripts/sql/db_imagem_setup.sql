-- =========================================================
-- db_imagem_setup.sql
-- Run after connecting to database: db_imagem
-- =========================================================

SET statement_timeout = 0;

DROP TABLE IF EXISTS laudos_imagem;
DROP TABLE IF EXISTS capturas_imagem;
DROP TABLE IF EXISTS equipamentos_imagem;
DROP TABLE IF EXISTS pacientes_seed;

CREATE TABLE equipamentos_imagem (
    equipamento_id      BIGSERIAL PRIMARY KEY,
    equipment_code      VARCHAR(20) NOT NULL UNIQUE,
    modality            TEXT NOT NULL,
    site_name           TEXT NOT NULL,
    municipality_code   INTEGER NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_equipamentos_modality ON equipamentos_imagem(modality);

INSERT INTO equipamentos_imagem (equipment_code, modality, site_name, municipality_code)
SELECT
    'IMG' || LPAD(gs::TEXT, 5, '0'),
    CASE
        WHEN gs % 3 = 0 THEN 'dermoscopy'
        WHEN gs % 3 = 1 THEN 'macroscopy'
        ELSE 'confocal'
    END,
    'site_' || gs,
    100000 + (gs % 40)
FROM generate_series(1, 110) AS gs;

CREATE TABLE pacientes_seed (
    patient_id          BIGSERIAL PRIMARY KEY,
    cpf                 VARCHAR(11) NOT NULL UNIQUE,
    patient_name        TEXT NOT NULL,
    sex                 CHAR(1) NOT NULL CHECK (sex IN ('M', 'F')),
    birth_date          DATE NOT NULL,
    municipality_code   INTEGER NOT NULL,
    imaging_risk_score  NUMERIC(5,2) NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_pacientes_cpf ON pacientes_seed(cpf);

INSERT INTO pacientes_seed (cpf, patient_name, sex, birth_date, municipality_code, imaging_risk_score)
SELECT
    LPAD(gs::TEXT, 11, '0'),
    'patient_' || gs,
    CASE WHEN gs % 2 = 0 THEN 'F' ELSE 'M' END,
    DATE '1958-01-01' + (((gs * 29) % 20000)::int),
    100000 + (gs % 40),
    ROUND((5 + ((gs * 13) % 95))::NUMERIC, 2)
FROM generate_series(1, 10000) AS gs;

CREATE TABLE capturas_imagem (
    image_capture_id    BIGSERIAL PRIMARY KEY,
    cpf                 VARCHAR(11) NOT NULL,
    equipamento_id      BIGINT NOT NULL REFERENCES equipamentos_imagem(equipamento_id),
    captured_at         DATE NOT NULL,
    body_site           TEXT NOT NULL,
    suspicion_score     NUMERIC(5,2) NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_capturas_cpf ON capturas_imagem(cpf);
CREATE INDEX idx_capturas_date ON capturas_imagem(captured_at);
CREATE INDEX idx_capturas_score ON capturas_imagem(suspicion_score);
CREATE INDEX idx_capturas_body_site ON capturas_imagem(body_site);

INSERT INTO capturas_imagem (
    cpf, equipamento_id, captured_at, body_site, suspicion_score
)
SELECT
    p.cpf,
    (1 + ((p.patient_id + g.n) % 110))::bigint,
    DATE '2024-01-01' + ((((p.patient_id * 3) + (g.n * 29)) % 540)::int),
    CASE
        WHEN p.patient_id % 5 = 0 THEN 'face'
        WHEN p.patient_id % 5 = 1 THEN 'torso'
        WHEN p.patient_id % 5 = 2 THEN 'arm'
        WHEN p.patient_id % 5 = 3 THEN 'leg'
        ELSE 'scalp'
    END,
    ROUND((0.10 + (((p.patient_id * 3 + g.n * 7) % 90) / 100.0))::NUMERIC, 2)
FROM pacientes_seed p
JOIN LATERAL generate_series(
    1,
    CASE WHEN p.patient_id % 10 < 5 THEN 1 + (p.patient_id % 2) ELSE 0 END
) AS g(n) ON TRUE;

CREATE TABLE laudos_imagem (
    report_id           BIGSERIAL PRIMARY KEY,
    image_capture_id    BIGINT NOT NULL REFERENCES capturas_imagem(image_capture_id),
    cpf                 VARCHAR(11) NOT NULL,
    lesion_pattern      TEXT NOT NULL,
    report_status       TEXT NOT NULL,
    signed_at           DATE NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

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
        WHEN (c.image_capture_id + g.n) % 4 = 0 THEN 'asymmetric'
        WHEN (c.image_capture_id + g.n) % 4 = 1 THEN 'symmetric'
        WHEN (c.image_capture_id + g.n) % 4 = 2 THEN 'multicomponent'
        ELSE 'homogeneous'
    END,
    CASE
        WHEN (c.image_capture_id + g.n) % 5 = 0 THEN 'review_required'
        WHEN (c.image_capture_id + g.n) % 5 = 1 THEN 'critical'
        ELSE 'final'
    END,
    c.captured_at + (((g.n - 1) % 3)::int)
FROM capturas_imagem c
JOIN LATERAL generate_series(1, 1) AS g(n) ON TRUE;

ANALYZE equipamentos_imagem;
ANALYZE pacientes_seed;
ANALYZE capturas_imagem;
ANALYZE laudos_imagem;
