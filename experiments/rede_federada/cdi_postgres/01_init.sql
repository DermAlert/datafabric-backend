-- =============================================================================
-- CDI — Centro de Diagnóstico por Imagem
-- Engine: PostgreSQL 15
-- Banco:  cdi_imagem
--
-- Schemas:
--   pacientes  — cadastro central e vínculo de convênio
--   exames     — equipamentos, ordens de exame e laudos radiológicos
--   dosimetria — dose de radiação e contraste administrado
--
-- Elo de federação: cpf_pessoa (CHAR 11, sem máscara)
-- Topologia: CDI é o HUB — compartilha CPFs com REH e com RFA.
--
-- Schema + seed NOMINAL pequeno. Volume de 500k em seeds/cdi_bulk.sql.
-- =============================================================================

SET statement_timeout = 0;
SET client_encoding   = 'UTF8';

-- =============================================================================
-- SCHEMA: pacientes
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS pacientes;

DROP TABLE IF EXISTS pacientes.convenio_vinculo CASCADE;
DROP TABLE IF EXISTS pacientes.cadastro_paciente CASCADE;

CREATE TABLE pacientes.cadastro_paciente (
    paciente_id      SERIAL        PRIMARY KEY,
    cpf_pessoa       CHAR(11)      NOT NULL
                                   CONSTRAINT uq_cdi_cpf UNIQUE,
    nome_completo    VARCHAR(150)  NOT NULL,
    data_nascimento  DATE          NOT NULL,
    sexo_biologico   CHAR(1)       CHECK (sexo_biologico IN ('M','F','I')),
    peso_kg          NUMERIC(5,2),
    altura_cm        NUMERIC(5,1),
    alergia_contraste BOOLEAN      NOT NULL DEFAULT FALSE,
    telefone         VARCHAR(15),
    municipio        VARCHAR(80),
    uf_sigla         CHAR(2),
    cadastrado_em    TIMESTAMPTZ   NOT NULL DEFAULT now()
);
COMMENT ON COLUMN pacientes.cadastro_paciente.cpf_pessoa IS 'CPF sem máscara — chave de federação (hub REH↔CDI↔RFA)';

CREATE TABLE pacientes.convenio_vinculo (
    vinculo_id       SERIAL        PRIMARY KEY,
    paciente_id      INT           NOT NULL
                     REFERENCES pacientes.cadastro_paciente(paciente_id) ON DELETE CASCADE,
    operadora_ans    VARCHAR(20)   NOT NULL,
    nome_plano       VARCHAR(80),
    tipo_acomodacao  VARCHAR(20)   CHECK (tipo_acomodacao IN ('enfermaria','apartamento','particular')),
    carteirinha      VARCHAR(30),
    validade         DATE
);

CREATE INDEX idx_cdi_pac_cpf   ON pacientes.cadastro_paciente(cpf_pessoa);
CREATE INDEX idx_cdi_vinc_pac  ON pacientes.convenio_vinculo(paciente_id);

-- =============================================================================
-- SCHEMA: exames
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS exames;

DROP TABLE IF EXISTS exames.laudo_radiologico CASCADE;
DROP TABLE IF EXISTS exames.ordem_exame       CASCADE;
DROP TABLE IF EXISTS exames.equipamento       CASCADE;

CREATE TABLE exames.equipamento (
    equip_id        SERIAL       PRIMARY KEY,
    modalidade      VARCHAR(10)  NOT NULL
                    CHECK (modalidade IN ('TC','RM','RX','US','MAMO','PET','DENSIT')),
    fabricante      VARCHAR(60),
    modelo          VARCHAR(60),
    sala            VARCHAR(20),
    tesla_ou_canais VARCHAR(20),
    ativo           BOOLEAN      NOT NULL DEFAULT TRUE
);

CREATE TABLE exames.ordem_exame (
    ordem_id            SERIAL       PRIMARY KEY,
    paciente_id         INT          NOT NULL
                        REFERENCES pacientes.cadastro_paciente(paciente_id) ON DELETE CASCADE,
    equip_id            INT          REFERENCES exames.equipamento(equip_id),
    modalidade          VARCHAR(10)  NOT NULL,
    regiao_anatomica    VARCHAR(80)  NOT NULL,
    medico_solicitante  VARCHAR(12),
    indicacao_clinica   VARCHAR(200),
    data_agendada       TIMESTAMPTZ  NOT NULL,
    data_realizada      TIMESTAMPTZ,
    status_ordem        VARCHAR(15)  NOT NULL DEFAULT 'agendado'
                        CHECK (status_ordem IN ('agendado','realizado','laudado','cancelado','falta')),
    prioridade          VARCHAR(10)  CHECK (prioridade IN ('rotina','urgente'))
);

CREATE TABLE exames.laudo_radiologico (
    laudo_id          SERIAL       PRIMARY KEY,
    ordem_id          INT          NOT NULL
                      REFERENCES exames.ordem_exame(ordem_id) ON DELETE CASCADE,
    radiologista_crm  VARCHAR(12)  NOT NULL,
    achados           TEXT,
    impressao         TEXT,
    sistema_classif   VARCHAR(10)  CHECK (sistema_classif IN ('BI-RADS','LI-RADS','PI-RADS','TI-RADS','Lung-RADS','nenhum')),
    categoria_classif VARCHAR(6),
    achado_critico    BOOLEAN      NOT NULL DEFAULT FALSE,
    data_laudo        TIMESTAMPTZ  NOT NULL
);

CREATE INDEX idx_cdi_ordem_pac   ON exames.ordem_exame(paciente_id);
CREATE INDEX idx_cdi_ordem_data  ON exames.ordem_exame(data_realizada);
CREATE INDEX idx_cdi_ordem_modal ON exames.ordem_exame(modalidade);
CREATE INDEX idx_cdi_laudo_ordem ON exames.laudo_radiologico(ordem_id);

-- =============================================================================
-- SCHEMA: dosimetria
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS dosimetria;

DROP TABLE IF EXISTS dosimetria.contraste_administrado CASCADE;
DROP TABLE IF EXISTS dosimetria.dose_radiacao          CASCADE;

CREATE TABLE dosimetria.dose_radiacao (
    dose_id         SERIAL        PRIMARY KEY,
    ordem_id        INT           NOT NULL
                    REFERENCES exames.ordem_exame(ordem_id) ON DELETE CASCADE,
    dose_mgy        NUMERIC(8,3),
    dlp_mgycm       NUMERIC(10,3),
    kvp             SMALLINT,
    mas             NUMERIC(8,2),
    dose_efetiva_msv NUMERIC(8,4),
    registrado_em   TIMESTAMPTZ   NOT NULL
);

CREATE TABLE dosimetria.contraste_administrado (
    contraste_id    SERIAL        PRIMARY KEY,
    ordem_id        INT           NOT NULL
                    REFERENCES exames.ordem_exame(ordem_id) ON DELETE CASCADE,
    agente          VARCHAR(60)   NOT NULL,
    volume_ml       NUMERIC(6,1),
    via             VARCHAR(15)   CHECK (via IN ('EV','VO','retal','intra_articular')),
    reacao_adversa  VARCHAR(20)   CHECK (reacao_adversa IN ('nenhuma','leve','moderada','grave')),
    administrado_em TIMESTAMPTZ   NOT NULL
);

CREATE INDEX idx_cdi_dose_ordem  ON dosimetria.dose_radiacao(ordem_id);
CREATE INDEX idx_cdi_contr_ordem ON dosimetria.contraste_administrado(ordem_id);

-- =============================================================================
-- SEED NOMINAL. IDs em banda alta (900000001+).
--
-- CPFs compartilhados com REH : 11111000001, 11111000002
-- CPFs compartilhados com RFA : 22222000001, 22222000002
-- CPF exclusivo do CDI        : 44444000001
-- =============================================================================
INSERT INTO pacientes.cadastro_paciente
    (paciente_id, cpf_pessoa, nome_completo, data_nascimento, sexo_biologico, peso_kg, altura_cm, alergia_contraste, municipio, uf_sigla)
VALUES
    (900000001, '11111000001', 'João Batista Severino',   '1970-02-11', 'M', 84.0, 175.0, FALSE, 'Brasília', 'DF'),
    (900000002, '11111000002', 'Marta Ribeiro Aguiar',    '1985-06-30', 'F', 62.5, 163.0, TRUE,  'Brasília', 'DF'),
    (900000003, '22222000001', 'Otávio Camargo Pinto',    '1978-03-22', 'M', 91.0, 180.0, FALSE, 'Brasília', 'DF'),
    (900000004, '22222000002', 'Renata Vasconcelos Sá',   '1990-11-14', 'F', 70.0, 168.0, FALSE, 'Taguatinga','DF'),
    (900000005, '44444000001', 'Hélio Prado Drummond',    '1962-08-08', 'M', 78.0, 172.0, FALSE, 'Brasília', 'DF');

INSERT INTO pacientes.convenio_vinculo
    (paciente_id, operadora_ans, nome_plano, tipo_acomodacao, carteirinha, validade)
VALUES
    (900000001, 'ANS33445', 'Plano Saúde Total',   'apartamento', 'CT-0001', '2026-12-31'),
    (900000003, 'ANS77882', 'Vida Plena Premium',  'apartamento', 'CT-0003', '2025-10-31'),
    (900000005, 'ANS11009', 'BásicoMed',           'enfermaria',  'CT-0005', '2026-06-30');

INSERT INTO exames.equipamento (equip_id, modalidade, fabricante, modelo, sala, tesla_ou_canais) VALUES
    (900000001, 'TC',   'Siemens',  'SOMATOM go.Top', 'Sala 1', '128 canais'),
    (900000002, 'RM',   'GE',       'SIGNA Pioneer',  'Sala 2', '3.0 T'),
    (900000003, 'MAMO', 'Hologic',  'Selenia Dimensions', 'Sala 4', 'tomossíntese');

INSERT INTO exames.ordem_exame
    (ordem_id, paciente_id, equip_id, modalidade, regiao_anatomica, medico_solicitante, indicacao_clinica, data_agendada, data_realizada, status_ordem, prioridade)
VALUES
    (900000001, 900000001, 900000001, 'TC',   'cranio',         'CRM_EMG01', 'Pós-IAM, descartar evento neurológico', '2024-05-04 09:00:00-03', '2024-05-04 09:25:00-03', 'laudado', 'urgente'),
    (900000002, 900000003, 900000002, 'RM',   'prostata',       'CRM_URO11', 'PSA elevado, rastreamento',             '2024-05-20 14:00:00-03', '2024-05-20 14:40:00-03', 'laudado', 'rotina'),
    (900000003, 900000004, 900000003, 'MAMO', 'mama_bilateral', 'CRM_MAS22', 'Rastreamento mamográfico bienal',       '2024-06-02 10:00:00-03', '2024-06-02 10:15:00-03', 'laudado', 'rotina'),
    (900000004, 900000005, 900000001, 'TC',   'torax',          'CRM_PNE33', 'Nódulo pulmonar em investigação',       '2024-06-10 11:00:00-03', '2024-06-10 11:20:00-03', 'laudado', 'rotina');

INSERT INTO exames.laudo_radiologico
    (ordem_id, radiologista_crm, achados, impressao, sistema_classif, categoria_classif, achado_critico, data_laudo)
VALUES
    (900000001, 'CRM_RAD90', 'Ausência de sangramento intracraniano. Sem sinais de isquemia aguda.', 'Exame sem alterações agudas.', 'nenhum', NULL, FALSE, '2024-05-04 10:30:00-03'),
    (900000002, 'CRM_RAD91', 'Lesão PI-RADS 4 em zona periférica posterior à esquerda.', 'Achado suspeito — recomendar biópsia dirigida.', 'PI-RADS', '4', TRUE, '2024-05-20 17:00:00-03'),
    (900000003, 'CRM_RAD92', 'Mamas densas. Nódulo circunscrito BI-RADS 3 em QSE direito.', 'Provavelmente benigno — controle em 6 meses.', 'BI-RADS', '3', FALSE, '2024-06-02 15:00:00-03'),
    (900000004, 'CRM_RAD90', 'Nódulo pulmonar sólido de 9mm em LSD, Lung-RADS 4A.', 'Recomendar PET-CT / seguimento conforme protocolo.', 'Lung-RADS', '4A', TRUE, '2024-06-10 16:00:00-03');

INSERT INTO dosimetria.dose_radiacao
    (ordem_id, dose_mgy, dlp_mgycm, kvp, mas, dose_efetiva_msv, registrado_em)
VALUES
    (900000001, 58.300, 1024.500, 120, 280.0, 2.1500, '2024-05-04 09:25:00-03'),
    (900000003, 12.400,    NULL,   28,  64.0, 0.4200, '2024-06-02 10:15:00-03'),
    (900000004, 7.800,   312.700, 110, 110.0, 5.2000, '2024-06-10 11:20:00-03');

INSERT INTO dosimetria.contraste_administrado
    (ordem_id, agente, volume_ml, via, reacao_adversa, administrado_em)
VALUES
    (900000001, 'Iodado não-iônico (Iohexol 350)', 80.0, 'EV', 'nenhuma', '2024-05-04 09:20:00-03'),
    (900000002, 'Gadolínio (Gadoterato)',          15.0, 'EV', 'nenhuma', '2024-05-20 14:35:00-03'),
    (900000004, 'Iodado não-iônico (Iopamidol 370)',90.0,'EV', 'leve',    '2024-06-10 11:15:00-03');

SELECT setval('pacientes.cadastro_paciente_paciente_id_seq', 900000100, true);
SELECT setval('exames.equipamento_equip_id_seq',             900000100, true);
SELECT setval('exames.ordem_exame_ordem_id_seq',             900000100, true);
