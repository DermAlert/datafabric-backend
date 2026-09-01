-- =============================================================================
-- REH — Rede de Emergências Hospitalares
-- Engine: PostgreSQL 15
-- Banco:  reh_emergencias
--
-- Schemas:
--   pronto_socorro — paciente, atendimento e classificação de risco (Manchester)
--   internacao     — leitos, admissões hospitalares e procedimentos
--   monitoramento  — série temporal de sinais vitais e prescrições de urgência
--
-- Elo de federação: cpf_paciente (CHAR 11, sem máscara)
-- Topologia: REH compartilha CPFs com CDI; NÃO compartilha com RFA.
--
-- Este arquivo cria o schema + um seed NOMINAL pequeno (~6 pacientes p/ demo).
-- O volume de 500k é carregado à parte por seeds/reh_bulk.sql (load_bulk.sh).
-- =============================================================================

SET statement_timeout = 0;
SET client_encoding   = 'UTF8';

-- =============================================================================
-- SCHEMA: pronto_socorro
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS pronto_socorro;

DROP TABLE IF EXISTS pronto_socorro.classificacao_manchester CASCADE;
DROP TABLE IF EXISTS pronto_socorro.atendimento_emergencia   CASCADE;
DROP TABLE IF EXISTS pronto_socorro.paciente_emergencia      CASCADE;

-- Cadastro central do paciente atendido na emergência
CREATE TABLE pronto_socorro.paciente_emergencia (
    paciente_id        SERIAL        PRIMARY KEY,
    cpf_paciente       CHAR(11)      NOT NULL
                                     CONSTRAINT uq_paciente_cpf UNIQUE,
    nome_completo      VARCHAR(150)  NOT NULL,
    data_nascimento    DATE          NOT NULL,
    sexo_biologico     CHAR(1)       CHECK (sexo_biologico IN ('M','F','I')),
    tipo_sanguineo     VARCHAR(3)    CHECK (tipo_sanguineo IN ('A+','A-','B+','B-','AB+','AB-','O+','O-')),
    cartao_sus         VARCHAR(15),
    municipio          VARCHAR(80),
    uf_sigla           CHAR(2),
    cadastrado_em      TIMESTAMPTZ   NOT NULL DEFAULT now()
);
COMMENT ON COLUMN pronto_socorro.paciente_emergencia.cpf_paciente IS 'CPF sem máscara — chave de federação (REH↔CDI)';

-- Atendimento (uma passagem pelo PS)
CREATE TABLE pronto_socorro.atendimento_emergencia (
    atendimento_id     SERIAL        PRIMARY KEY,
    paciente_id        INT           NOT NULL
                       REFERENCES pronto_socorro.paciente_emergencia(paciente_id) ON DELETE CASCADE,
    data_entrada       TIMESTAMPTZ   NOT NULL,
    data_saida         TIMESTAMPTZ,
    via_chegada        VARCHAR(15)   CHECK (via_chegada IN ('proprio','ambulancia','samu','resgate','transferencia')),
    queixa_principal   VARCHAR(200)  NOT NULL,
    pa_sistolica_ini   SMALLINT,
    pa_diastolica_ini  SMALLINT,
    fc_inicial         SMALLINT,
    satO2_inicial      NUMERIC(4,1),
    temperatura_ini    NUMERIC(4,1),
    desfecho_atend     VARCHAR(20)   CHECK (desfecho_atend IN ('alta','internacao','obito','transferencia','evasao'))
);

-- Classificação de risco (protocolo Manchester) — 1 por atendimento
CREATE TABLE pronto_socorro.classificacao_manchester (
    classificacao_id   SERIAL        PRIMARY KEY,
    atendimento_id     INT           NOT NULL
                       REFERENCES pronto_socorro.atendimento_emergencia(atendimento_id) ON DELETE CASCADE,
    cor_risco          VARCHAR(10)   NOT NULL
                       CHECK (cor_risco IN ('vermelho','laranja','amarelo','verde','azul')),
    tempo_alvo_min     SMALLINT      NOT NULL,
    discriminador      VARCHAR(120),
    fluxograma         VARCHAR(80),
    enfermeiro_coren   VARCHAR(12),
    classificado_em    TIMESTAMPTZ   NOT NULL
);

CREATE INDEX idx_reh_pac_cpf    ON pronto_socorro.paciente_emergencia(cpf_paciente);
CREATE INDEX idx_reh_atend_pac  ON pronto_socorro.atendimento_emergencia(paciente_id);
CREATE INDEX idx_reh_atend_data ON pronto_socorro.atendimento_emergencia(data_entrada);
CREATE INDEX idx_reh_manch_at   ON pronto_socorro.classificacao_manchester(atendimento_id);
CREATE INDEX idx_reh_manch_cor  ON pronto_socorro.classificacao_manchester(cor_risco);

-- =============================================================================
-- SCHEMA: internacao
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS internacao;

DROP TABLE IF EXISTS internacao.procedimento_realizado CASCADE;
DROP TABLE IF EXISTS internacao.admissao_hospitalar    CASCADE;
DROP TABLE IF EXISTS internacao.leito                  CASCADE;

-- Catálogo de leitos
CREATE TABLE internacao.leito (
    leito_id     SERIAL       PRIMARY KEY,
    setor        VARCHAR(40)  NOT NULL,
    tipo_leito   VARCHAR(15)  NOT NULL CHECK (tipo_leito IN ('uti','semi_intensiva','enfermaria','isolamento')),
    numero       VARCHAR(10)  NOT NULL,
    ativo        BOOLEAN      NOT NULL DEFAULT TRUE,
    CONSTRAINT uq_leito UNIQUE (setor, numero)
);

-- Admissão hospitalar (atendimento que virou internação)
CREATE TABLE internacao.admissao_hospitalar (
    admissao_id     SERIAL       PRIMARY KEY,
    atendimento_id  INT          NOT NULL
                    REFERENCES pronto_socorro.atendimento_emergencia(atendimento_id),
    leito_id        INT          NOT NULL REFERENCES internacao.leito(leito_id),
    data_admissao   TIMESTAMPTZ  NOT NULL,
    data_alta       TIMESTAMPTZ,
    cid10_principal CHAR(4),
    desfecho        VARCHAR(15)  CHECK (desfecho IN ('alta','obito','transferencia','evasao')),
    diaria_custo    NUMERIC(10,2)
);

-- Procedimentos realizados durante a internação
CREATE TABLE internacao.procedimento_realizado (
    procedimento_id SERIAL       PRIMARY KEY,
    admissao_id     INT          NOT NULL
                    REFERENCES internacao.admissao_hospitalar(admissao_id) ON DELETE CASCADE,
    codigo_tuss     VARCHAR(12)  NOT NULL,
    descricao       VARCHAR(200) NOT NULL,
    equipe          VARCHAR(80),
    data_hora       TIMESTAMPTZ  NOT NULL,
    custo           NUMERIC(10,2)
);

CREATE INDEX idx_reh_adm_atend ON internacao.admissao_hospitalar(atendimento_id);
CREATE INDEX idx_reh_adm_leito ON internacao.admissao_hospitalar(leito_id);
CREATE INDEX idx_reh_proc_adm  ON internacao.procedimento_realizado(admissao_id);

-- =============================================================================
-- SCHEMA: monitoramento
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS monitoramento;

DROP TABLE IF EXISTS monitoramento.prescricao_urgencia CASCADE;
DROP TABLE IF EXISTS monitoramento.sinal_vital_serie   CASCADE;

-- Série temporal de sinais vitais (driver de volume)
CREATE TABLE monitoramento.sinal_vital_serie (
    vital_id        BIGSERIAL    PRIMARY KEY,
    admissao_id     INT          NOT NULL
                    REFERENCES internacao.admissao_hospitalar(admissao_id) ON DELETE CASCADE,
    aferido_em      TIMESTAMPTZ  NOT NULL,
    pa_sistolica    SMALLINT,
    pa_diastolica   SMALLINT,
    fc              SMALLINT,
    fr              SMALLINT,
    temperatura     NUMERIC(4,1),
    satO2           NUMERIC(4,1),
    glasgow         SMALLINT     CHECK (glasgow BETWEEN 3 AND 15),
    dor_eva         SMALLINT     CHECK (dor_eva BETWEEN 0 AND 10)
);

CREATE TABLE monitoramento.prescricao_urgencia (
    prescricao_id   BIGSERIAL    PRIMARY KEY,
    admissao_id     INT          NOT NULL
                    REFERENCES internacao.admissao_hospitalar(admissao_id) ON DELETE CASCADE,
    medicamento     VARCHAR(120) NOT NULL,
    dose            VARCHAR(40),
    via             VARCHAR(20)  CHECK (via IN ('EV','IM','VO','SC','inalatoria','retal')),
    frequencia      VARCHAR(40),
    prescrito_em    TIMESTAMPTZ  NOT NULL,
    medico_crm      VARCHAR(12)
);

CREATE INDEX idx_reh_vital_adm  ON monitoramento.sinal_vital_serie(admissao_id);
CREATE INDEX idx_reh_vital_data ON monitoramento.sinal_vital_serie(aferido_em);
CREATE INDEX idx_reh_presc_adm  ON monitoramento.prescricao_urgencia(admissao_id);

-- =============================================================================
-- SEED NOMINAL (para demo sem o bulk). IDs em banda alta (900000001+) para
-- não colidir com os IDs 1..500000 usados pelo bulk.
--
-- CPFs compartilhados com CDI : 11111000001, 11111000002
-- CPFs exclusivos do REH      : 33333000001, 33333000002
-- (NENHUM CPF compartilhado com RFA — respeita a topologia em cadeia)
-- =============================================================================
INSERT INTO pronto_socorro.paciente_emergencia
    (paciente_id, cpf_paciente, nome_completo, data_nascimento, sexo_biologico, tipo_sanguineo, cartao_sus, municipio, uf_sigla)
VALUES
    (900000001, '11111000001', 'João Batista Severino',      '1970-02-11', 'M', 'O+',  '700001000001', 'Brasília',       'DF'),
    (900000002, '11111000002', 'Marta Ribeiro Aguiar',       '1985-06-30', 'F', 'A+',  '700001000002', 'Brasília',       'DF'),
    (900000003, '33333000001', 'Sebastião Lopes da Mata',    '1959-09-19', 'M', 'B+',  '700001000003', 'Goiânia',        'GO'),
    (900000004, '33333000002', 'Cláudia Nunes Teixeira',     '1992-12-05', 'F', 'AB-', '700001000004', 'Anápolis',       'GO');

INSERT INTO pronto_socorro.atendimento_emergencia
    (atendimento_id, paciente_id, data_entrada, data_saida, via_chegada, queixa_principal,
     pa_sistolica_ini, pa_diastolica_ini, fc_inicial, satO2_inicial, temperatura_ini, desfecho_atend)
VALUES
    (900000001, 900000001, '2024-05-02 21:14:00-03', '2024-05-05 10:00:00-03', 'samu',   'Dor torácica com irradiação para MSE', 165, 100, 112, 94.0, 36.8, 'internacao'),
    (900000002, 900000002, '2024-05-10 08:40:00-03', '2024-05-10 13:20:00-03', 'proprio','Crise asmática moderada',               128,  82,  98, 91.5, 37.1, 'alta'),
    (900000003, 900000003, '2024-04-18 02:05:00-03', '2024-04-25 16:00:00-03', 'ambulancia','AVC isquêmico — déficit motor agudo', 188, 110,  88, 95.0, 36.5, 'internacao'),
    (900000004, 900000004, '2024-06-01 19:30:00-03', '2024-06-01 22:10:00-03', 'proprio','Cefaleia intensa súbita',               142,  90, 102, 98.0, 36.9, 'alta');

INSERT INTO pronto_socorro.classificacao_manchester
    (atendimento_id, cor_risco, tempo_alvo_min, discriminador, fluxograma, enfermeiro_coren, classificado_em)
VALUES
    (900000001, 'laranja',  10, 'Dor precordial / risco cardiovascular', 'Dor torácica',  'COREN001', '2024-05-02 21:18:00-03'),
    (900000002, 'amarelo',  60, 'Dispneia moderada, SatO2 91%',          'Dispneia',      'COREN002', '2024-05-10 08:46:00-03'),
    (900000003, 'vermelho',  0, 'Déficit neurológico agudo / protocolo AVC','Alteração neurológica','COREN001','2024-04-18 02:08:00-03'),
    (900000004, 'amarelo',  60, 'Cefaleia súbita intensa (thunderclap?)','Cefaleia',      'COREN003', '2024-06-01 19:36:00-03');

INSERT INTO internacao.leito (leito_id, setor, tipo_leito, numero) VALUES
    (900000001, 'UTI Adulto',        'uti',        'U-12'),
    (900000002, 'Clínica Médica 3',  'enfermaria', 'CM3-08'),
    (900000003, 'UTI Neuro',         'uti',        'N-04');

INSERT INTO internacao.admissao_hospitalar
    (admissao_id, atendimento_id, leito_id, data_admissao, data_alta, cid10_principal, desfecho, diaria_custo)
VALUES
    (900000001, 900000001, 900000001, '2024-05-02 23:50:00-03', '2024-05-05 10:00:00-03', 'I21', 'alta', 3200.00),
    (900000002, 900000003, 900000003, '2024-04-18 05:30:00-03', '2024-04-25 16:00:00-03', 'I63', 'alta', 3850.00);

INSERT INTO internacao.procedimento_realizado
    (admissao_id, codigo_tuss, descricao, equipe, data_hora, custo)
VALUES
    (900000001, '30912017', 'Cateterismo cardíaco com angioplastia + stent', 'Hemodinâmica',   '2024-05-03 02:10:00-03', 18500.00),
    (900000002, '41001010', 'Trombólise endovenosa (rt-PA) no AVC isquêmico', 'Neurologia',     '2024-04-18 06:05:00-03',  9200.00);

INSERT INTO monitoramento.sinal_vital_serie
    (admissao_id, aferido_em, pa_sistolica, pa_diastolica, fc, fr, temperatura, satO2, glasgow, dor_eva)
VALUES
    (900000001, '2024-05-03 00:00:00-03', 150, 95, 104, 22, 36.9, 95.0, 15, 7),
    (900000001, '2024-05-03 06:00:00-03', 132, 84,  88, 18, 36.6, 97.0, 15, 3),
    (900000002, '2024-04-18 06:00:00-03', 178,108,  82, 16, 36.4, 96.0, 11, 0),
    (900000002, '2024-04-18 12:00:00-03', 156, 96,  78, 16, 36.7, 97.0, 13, 0);

INSERT INTO monitoramento.prescricao_urgencia
    (admissao_id, medicamento, dose, via, frequencia, prescrito_em, medico_crm)
VALUES
    (900000001, 'AAS', '300mg', 'VO', 'dose única',     '2024-05-02 23:55:00-03', 'CRM_EMG01'),
    (900000001, 'Heparina', '5000 UI', 'EV', '8/8h',    '2024-05-03 00:10:00-03', 'CRM_EMG01'),
    (900000002, 'Alteplase', '0.9mg/kg', 'EV', 'protocolo','2024-04-18 06:05:00-03', 'CRM_EMG02');

-- Ajusta sequences para não colidir com a banda alta do seed nominal
SELECT setval('pronto_socorro.paciente_emergencia_paciente_id_seq',      900000100, true);
SELECT setval('pronto_socorro.atendimento_emergencia_atendimento_id_seq',900000100, true);
SELECT setval('internacao.leito_leito_id_seq',                           900000100, true);
SELECT setval('internacao.admissao_hospitalar_admissao_id_seq',          900000100, true);
