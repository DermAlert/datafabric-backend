-- =========================================================
-- db_farmacia_setup.sql
-- Run after connecting to database: db_farmacia
-- =========================================================

SET statement_timeout = 0;

DROP TABLE IF EXISTS itens_dispensacao;
DROP TABLE IF EXISTS dispensacoes;
DROP TABLE IF EXISTS unidades_farmacia;
DROP TABLE IF EXISTS pacientes_seed;

CREATE TABLE unidades_farmacia (
    farmacia_id         BIGSERIAL PRIMARY KEY,
    pharmacy_code       VARCHAR(20) NOT NULL UNIQUE,
    pharmacy_name       TEXT NOT NULL,
    municipality_code   INTEGER NOT NULL,
    channel_type        TEXT NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_unidades_farmacia_municipality ON unidades_farmacia(municipality_code);

INSERT INTO unidades_farmacia (pharmacy_code, pharmacy_name, municipality_code, channel_type)
SELECT
    'FAR' || LPAD(gs::TEXT, 5, '0'),
    'farmacia_' || gs,
    100000 + (gs % 40),
    CASE
        WHEN gs % 3 = 0 THEN 'alto_custo'
        WHEN gs % 3 = 1 THEN 'hospitalar'
        ELSE 'ambulatorial'
    END
FROM generate_series(1, 95) AS gs;

CREATE TABLE pacientes_seed (
    patient_id          BIGSERIAL PRIMARY KEY,
    cpf                 VARCHAR(11) NOT NULL UNIQUE,
    patient_name        TEXT NOT NULL,
    sex                 CHAR(1) NOT NULL CHECK (sex IN ('M', 'F')),
    birth_date          DATE NOT NULL,
    municipality_code   INTEGER NOT NULL,
    adherence_score     NUMERIC(5,2) NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_pacientes_cpf ON pacientes_seed(cpf);

INSERT INTO pacientes_seed (cpf, patient_name, sex, birth_date, municipality_code, adherence_score)
SELECT
    LPAD(gs::TEXT, 11, '0'),
    'patient_' || gs,
    CASE WHEN gs % 2 = 0 THEN 'F' ELSE 'M' END,
    DATE '1958-01-01' + (((gs * 29) % 20000)::int),
    100000 + (gs % 40),
    ROUND((10 + ((gs * 19) % 90))::NUMERIC, 2)
FROM generate_series(1, 10000) AS gs;

CREATE TABLE dispensacoes (
    dispensing_id       BIGSERIAL PRIMARY KEY,
    cpf                 VARCHAR(11) NOT NULL,
    farmacia_id         BIGINT NOT NULL REFERENCES unidades_farmacia(farmacia_id),
    dispense_date       DATE NOT NULL,
    authorization_type  TEXT NOT NULL,
    total_items         INTEGER NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dispensacoes_cpf ON dispensacoes(cpf);
CREATE INDEX idx_dispensacoes_date ON dispensacoes(dispense_date);
CREATE INDEX idx_dispensacoes_auth ON dispensacoes(authorization_type);

INSERT INTO dispensacoes (
    cpf, farmacia_id, dispense_date, authorization_type, total_items
)
SELECT
    p.cpf,
    (1 + ((p.patient_id + g.n) % 95))::bigint,
    DATE '2024-01-01' + ((((p.patient_id * 11) + (g.n * 23)) % 540)::int),
    CASE
        WHEN p.patient_id % 4 = 0 THEN 'sus'
        WHEN p.patient_id % 4 = 1 THEN 'judicial'
        WHEN p.patient_id % 4 = 2 THEN 'private'
        ELSE 'insurance'
    END,
    1 + ((p.patient_id + g.n) % 4)
FROM pacientes_seed p
JOIN LATERAL generate_series(
    1,
    CASE WHEN p.patient_id % 10 < 5 THEN 1 + (p.patient_id % 3) ELSE 0 END
) AS g(n) ON TRUE;

CREATE TABLE itens_dispensacao (
    item_dispense_id    BIGSERIAL PRIMARY KEY,
    dispensing_id       BIGINT NOT NULL REFERENCES dispensacoes(dispensing_id),
    cpf                 VARCHAR(11) NOT NULL,
    drug_code           VARCHAR(20) NOT NULL,
    quantity            INTEGER NOT NULL,
    unit_cost           NUMERIC(10,2) NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_itens_dispensacao_dispensing ON itens_dispensacao(dispensing_id);
CREATE INDEX idx_itens_dispensacao_cpf ON itens_dispensacao(cpf);
CREATE INDEX idx_itens_dispensacao_drug ON itens_dispensacao(drug_code);

INSERT INTO itens_dispensacao (
    dispensing_id, cpf, drug_code, quantity, unit_cost
)
SELECT
    d.dispensing_id,
    d.cpf,
    'MED_' || LPAD((((d.dispensing_id % 140) + g.n))::TEXT, 4, '0'),
    1 + ((d.dispensing_id + g.n) % 4),
    ROUND((40 + ((d.dispensing_id * g.n) % 600))::NUMERIC, 2)
FROM dispensacoes d
JOIN LATERAL generate_series(1, d.total_items) AS g(n) ON TRUE;

ANALYZE unidades_farmacia;
ANALYZE pacientes_seed;
ANALYZE dispensacoes;
ANALYZE itens_dispensacao;
