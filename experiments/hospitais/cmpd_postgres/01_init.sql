-- =============================================================================
-- Hospital B — Centro de Medicina Preventiva e Diagnóstico (CMPD)
-- Engine: PostgreSQL 15
-- Banco:  cmpd_preventiva
--
-- Schemas criados:
--   cadastro_beneficiario — perfil, antecedentes e sinais vitais
--   rastreamento          — programas de prevenção e inscrições
--   laboratorio           — requisições, resultados e achados de imagem
--
-- Elo de federação com IDA: numero_cpf (CHAR 11, sem máscara)
-- =============================================================================

SET statement_timeout = 0;
SET client_encoding   = 'UTF8';

-- =============================================================================
-- SCHEMA: cadastro_beneficiario
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS cadastro_beneficiario;

DROP TABLE IF EXISTS cadastro_beneficiario.indicadores_vitais   CASCADE;
DROP TABLE IF EXISTS cadastro_beneficiario.antecedentes_clinicos CASCADE;
DROP TABLE IF EXISTS cadastro_beneficiario.perfil_saude         CASCADE;

-- -----------------------------------------------------------------------
-- Cadastro central do beneficiário
-- -----------------------------------------------------------------------
CREATE TABLE cadastro_beneficiario.perfil_saude (
    beneficiario_id          SERIAL          PRIMARY KEY,
    numero_cpf               CHAR(11)        NOT NULL
                                             CONSTRAINT uq_perfil_cpf UNIQUE,
    prenome                  VARCHAR(60)     NOT NULL,
    sobrenome                VARCHAR(90)     NOT NULL,
    nascimento               DATE            NOT NULL,
    genero_identidade        VARCHAR(30),
    telefone_celular         VARCHAR(15),
    email_contato            VARCHAR(120),
    cep_residencia           CHAR(8),
    municipio                VARCHAR(80),
    uf_sigla                 CHAR(2),
    plano_saude_ans          VARCHAR(20),     -- Registro ANS do plano
    renda_faixa              VARCHAR(2)      CHECK (renda_faixa IN ('A','B','C','D','E')),
    fumante_status           VARCHAR(15)     CHECK (fumante_status IN ('nunca','ex','atual')),
    pratica_atividade_fisica BOOLEAN         NOT NULL DEFAULT FALSE,
    imc_atual                NUMERIC(5,2)
);

COMMENT ON TABLE  cadastro_beneficiario.perfil_saude IS 'Cadastro central do beneficiário do CMPD';
COMMENT ON COLUMN cadastro_beneficiario.perfil_saude.numero_cpf IS 'CPF sem máscara — chave de federação com IDA';

-- -----------------------------------------------------------------------
-- Histórico de condições preexistentes
-- -----------------------------------------------------------------------
CREATE TABLE cadastro_beneficiario.antecedentes_clinicos (
    antecedente_id       SERIAL          PRIMARY KEY,
    beneficiario_id      INT             NOT NULL
                         REFERENCES cadastro_beneficiario.perfil_saude(beneficiario_id)
                         ON DELETE CASCADE,
    grupo_cid11          VARCHAR(10)     NOT NULL,
    descricao_condicao   VARCHAR(200)    NOT NULL,
    ano_diagnostico      SMALLINT,
    em_tratamento_ativo  BOOLEAN         NOT NULL DEFAULT FALSE,
    medicacao_continua   VARCHAR(200),
    gravidade            VARCHAR(10)     CHECK (gravidade IN ('leve','moderada','grave'))
);

COMMENT ON TABLE cadastro_beneficiario.antecedentes_clinicos IS 'Histórico de condições clínicas preexistentes';

-- -----------------------------------------------------------------------
-- Série histórica de sinais vitais e bioquímica básica
-- -----------------------------------------------------------------------
CREATE TABLE cadastro_beneficiario.indicadores_vitais (
    vital_id           SERIAL          PRIMARY KEY,
    beneficiario_id    INT             NOT NULL
                       REFERENCES cadastro_beneficiario.perfil_saude(beneficiario_id),
    data_aferimento    TIMESTAMPTZ     NOT NULL,
    pressao_sistolica  SMALLINT        CHECK (pressao_sistolica  BETWEEN 40 AND 280),
    pressao_diastolica SMALLINT        CHECK (pressao_diastolica BETWEEN 20 AND 180),
    glicemia_jejum_mg  NUMERIC(6,2),
    colesterol_total   NUMERIC(6,2),
    hdl                NUMERIC(5,2),
    ldl                NUMERIC(5,2),
    triglicerideos     NUMERIC(6,2),
    saturacao_o2       NUMERIC(4,1)    CHECK (saturacao_o2 BETWEEN 0 AND 100),
    peso_kg            NUMERIC(6,2),
    altura_cm          NUMERIC(5,1)
);

COMMENT ON TABLE cadastro_beneficiario.indicadores_vitais IS 'Série histórica de sinais vitais e bioquímica básica';

CREATE INDEX idx_ps_cpf         ON cadastro_beneficiario.perfil_saude(numero_cpf);
CREATE INDEX idx_ac_benef       ON cadastro_beneficiario.antecedentes_clinicos(beneficiario_id);
CREATE INDEX idx_iv_benef_data  ON cadastro_beneficiario.indicadores_vitais(beneficiario_id, data_aferimento);


-- =============================================================================
-- SCHEMA: rastreamento
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS rastreamento;

DROP TABLE IF EXISTS rastreamento.inscricao_programa  CASCADE;
DROP TABLE IF EXISTS rastreamento.programa_prevencao  CASCADE;

-- -----------------------------------------------------------------------
-- Catálogo de programas de rastreamento disponíveis no CMPD
-- -----------------------------------------------------------------------
CREATE TABLE rastreamento.programa_prevencao (
    programa_id          SERIAL          PRIMARY KEY,
    denominacao          VARCHAR(120)    NOT NULL UNIQUE,
    orgao_alvo           VARCHAR(60)     NOT NULL,
    faixa_etaria_min     SMALLINT        NOT NULL,
    faixa_etaria_max     SMALLINT,
    periodicidade_meses  SMALLINT        NOT NULL,
    metodo_exame         VARCHAR(200)    NOT NULL,
    criterio_inclusao    TEXT,
    vigente              BOOLEAN         NOT NULL DEFAULT TRUE
);

COMMENT ON TABLE rastreamento.programa_prevencao IS 'Catálogo de programas de rastreamento preventivo';
COMMENT ON COLUMN rastreamento.programa_prevencao.metodo_exame IS 'Ex: colonoscopia, mamografia, LDCT, PSA sérico';

-- -----------------------------------------------------------------------
-- Inscrição de um beneficiário em um programa de rastreamento
-- -----------------------------------------------------------------------
CREATE TABLE rastreamento.inscricao_programa (
    inscricao_id       SERIAL          PRIMARY KEY,
    beneficiario_id    INT             NOT NULL
                       REFERENCES cadastro_beneficiario.perfil_saude(beneficiario_id),
    programa_id        INT             NOT NULL
                       REFERENCES rastreamento.programa_prevencao(programa_id),
    data_ingresso      DATE            NOT NULL DEFAULT CURRENT_DATE,
    proxima_realizacao DATE,
    risco_calculado    VARCHAR(15)     CHECK (risco_calculado IN ('baixo','intermediario','alto','muito_alto')),
    motivo_ingresso    TEXT,
    CONSTRAINT uq_inscricao UNIQUE (beneficiario_id, programa_id)
);

COMMENT ON TABLE rastreamento.inscricao_programa IS 'Inscrição de beneficiários em programas preventivos';

CREATE INDEX idx_insc_benef   ON rastreamento.inscricao_programa(beneficiario_id);
CREATE INDEX idx_insc_prog    ON rastreamento.inscricao_programa(programa_id);


-- =============================================================================
-- SCHEMA: laboratorio
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS laboratorio;

DROP TABLE IF EXISTS laboratorio.achado_imagem    CASCADE;
DROP TABLE IF EXISTS laboratorio.resultado_exame  CASCADE;
DROP TABLE IF EXISTS laboratorio.requisicao_exame CASCADE;

-- -----------------------------------------------------------------------
-- Requisição de exame emitida pelo médico preventivista
-- -----------------------------------------------------------------------
CREATE TABLE laboratorio.requisicao_exame (
    requisicao_id       SERIAL          PRIMARY KEY,
    beneficiario_id     INT             NOT NULL
                        REFERENCES cadastro_beneficiario.perfil_saude(beneficiario_id),
    medico_requisitante VARCHAR(12)     NOT NULL,
    painel_exames       JSONB           NOT NULL,
    prioridade          VARCHAR(10)     NOT NULL DEFAULT 'rotina'
                        CHECK (prioridade IN ('rotina','urgente')),
    inscricao_id        INT             REFERENCES rastreamento.inscricao_programa(inscricao_id),
    data_emissao        DATE            NOT NULL DEFAULT CURRENT_DATE,
    instrucoes_preparo  TEXT
);

COMMENT ON TABLE  laboratorio.requisicao_exame IS 'Requisições de exame emitidas por médicos preventivistas';
COMMENT ON COLUMN laboratorio.requisicao_exame.painel_exames IS 'Array JSONB de analitos: ["hemograma","PSA","CEA","CA-125"]';

-- -----------------------------------------------------------------------
-- Resultado individual por analito laboratorial
-- -----------------------------------------------------------------------
CREATE TABLE laboratorio.resultado_exame (
    resultado_id            SERIAL          PRIMARY KEY,
    requisicao_id           INT             NOT NULL
                            REFERENCES laboratorio.requisicao_exame(requisicao_id),
    analito                 VARCHAR(80)     NOT NULL,
    valor_numerico          NUMERIC(12,4),
    unidade_medida          VARCHAR(20),
    valor_referencia_min    NUMERIC(12,4),
    valor_referencia_max    NUMERIC(12,4),
    interpretacao           VARCHAR(20)
                            CHECK (interpretacao IN ('normal','alterado_leve','alterado_moderado','critico')),
    metodo_analitico        VARCHAR(60),
    data_coleta             TIMESTAMPTZ     NOT NULL,
    data_resultado          TIMESTAMPTZ,
    laboratorio_executante  VARCHAR(80)
);

COMMENT ON TABLE laboratorio.resultado_exame IS 'Resultados individuais por analito laboratorial';

-- -----------------------------------------------------------------------
-- Achados em exames de imagem preventivos
-- -----------------------------------------------------------------------
CREATE TABLE laboratorio.achado_imagem (
    achado_id              SERIAL          PRIMARY KEY,
    requisicao_id          INT             NOT NULL
                           REFERENCES laboratorio.requisicao_exame(requisicao_id),
    modalidade_imagem      VARCHAR(20)
                           CHECK (modalidade_imagem IN (
                               'ultrassonografia','tomografia','ressonancia',
                               'mamografia','rx','densitometria','ecocardiograma')),
    regiao_anatomica       VARCHAR(80)     NOT NULL,
    classificacao_bi_rads  SMALLINT        CHECK (classificacao_bi_rads BETWEEN 0 AND 6),
    descricao_radiologica  TEXT            NOT NULL,
    recomendacao           TEXT,
    laudado_por            VARCHAR(12)     NOT NULL,
    data_laudo_imagem      DATE            NOT NULL
);

COMMENT ON TABLE laboratorio.achado_imagem IS 'Achados em exames de imagem preventivos (US, TC, RM, mamografia)';
COMMENT ON COLUMN laboratorio.achado_imagem.classificacao_bi_rads IS 'Usado principalmente para mamografia (ACR BI-RADS 0-6)';

CREATE INDEX idx_req_benef   ON laboratorio.requisicao_exame(beneficiario_id);
CREATE INDEX idx_req_data    ON laboratorio.requisicao_exame(data_emissao);
CREATE INDEX idx_res_req     ON laboratorio.resultado_exame(requisicao_id);
CREATE INDEX idx_res_analito ON laboratorio.resultado_exame(analito);
CREATE INDEX idx_img_req     ON laboratorio.achado_imagem(requisicao_id);


-- =============================================================================
-- SEED DATA — dados sintéticos realistas
-- Pacientes compartilhados com IDA (mesmo CPF):
--   12345678901 | João Carlos Ferreira Machado
--   98765432100 | Ana Paula Santos Lima
--   55544433322 | Carlos Eduardo Oliveira Neto
-- Pacientes exclusivos do CMPD:
--   44455566677 | Fernanda Cristina Rodrigues
--   77788899900 | Antônio José Nascimento Silva
-- =============================================================================

-- -----------------------------------------------------------------------
-- cadastro_beneficiario.perfil_saude
-- -----------------------------------------------------------------------
INSERT INTO cadastro_beneficiario.perfil_saude
    (numero_cpf, prenome, sobrenome, nascimento, genero_identidade,
     telefone_celular, email_contato, cep_residencia, municipio, uf_sigla,
     plano_saude_ans, renda_faixa, fumante_status, pratica_atividade_fisica, imc_atual)
VALUES
    ('12345678901', 'João Carlos',   'Ferreira Machado',  '1975-03-15', 'masculino',
     '61999110001', 'joao.machado@email.com',   '70000001', 'Brasília',       'DF',
     'ANS12345', 'C', 'ex',   FALSE, 27.4),
    ('98765432100', 'Ana Paula',     'Santos Lima',       '1968-07-22', 'feminino',
     '11988220002', 'ana.lima@email.com',        '01310100', 'São Paulo',      'SP',
     'ANS67890', 'B', 'nunca', FALSE, 31.2),
    ('55544433322', 'Carlos Eduardo','Oliveira Neto',     '1982-11-08', 'masculino',
     '21977330003', 'carlos.neto@email.com',     '20040020', 'Rio de Janeiro', 'RJ',
     'ANS11223', 'C', 'nunca', TRUE,  23.8),
    ('44455566677', 'Fernanda Cristina', 'Rodrigues',     '1978-09-14', 'feminino',
     '31966440004', 'fernanda.rodrigues@email.com', '30130110', 'Belo Horizonte', 'MG',
     NULL,       'D', 'nunca', TRUE,  22.5),
    ('77788899900', 'Antônio José',  'Nascimento Silva',  '1963-04-25', 'masculino',
     '81955550005', 'antonio.silva@email.com',   '50050020', 'Recife',         'PE',
     'ANS44556', 'C', 'ex',   FALSE, 29.7);

-- -----------------------------------------------------------------------
-- cadastro_beneficiario.antecedentes_clinicos
-- -----------------------------------------------------------------------
INSERT INTO cadastro_beneficiario.antecedentes_clinicos
    (beneficiario_id, grupo_cid11, descricao_condicao, ano_diagnostico,
     em_tratamento_ativo, medicacao_continua, gravidade)
VALUES
    -- João: hipertensão arterial
    (1, 'BA00', 'Hipertensão arterial sistêmica', 2018,
     TRUE, 'Losartana 50mg/dia, Hidroclorotiazida 25mg/dia', 'moderada'),
    -- Ana Paula: diabetes tipo 2 + obesidade
    (2, '5A11', 'Diabetes mellitus tipo 2', 2015,
     TRUE, 'Metformina 1000mg 2x/dia, Empagliflozina 10mg/dia', 'grave'),
    (2, '5B81', 'Obesidade grau I (IMC 31.2)', 2015,
     TRUE, NULL, 'leve'),
    -- Carlos: sem antecedentes relevantes
    -- Fernanda: hipotireoidismo
    (4, '5A00', 'Hipotireoidismo primário (Tireoidite de Hashimoto)', 2012,
     TRUE, 'Levotiroxina 75mcg/dia em jejum', 'leve'),
    -- Antônio: insuficiência cardíaca + HAS
    (5, 'BA80', 'Insuficiência cardíaca com fração de ejeção reduzida (ICFEr)', 2020,
     TRUE, 'Carvedilol 25mg 2x/dia, Sacubitril/Valsartana 200mg 2x/dia, Espironolactona 25mg/dia', 'grave'),
    (5, 'BA00', 'Hipertensão arterial sistêmica', 2014,
     TRUE, 'Incluída no esquema da IC', 'moderada');

-- -----------------------------------------------------------------------
-- cadastro_beneficiario.indicadores_vitais
-- -----------------------------------------------------------------------
INSERT INTO cadastro_beneficiario.indicadores_vitais
    (beneficiario_id, data_aferimento, pressao_sistolica, pressao_diastolica,
     glicemia_jejum_mg, colesterol_total, hdl, ldl, triglicerideos,
     saturacao_o2, peso_kg, altura_cm)
VALUES
    -- João — visita 2024-03
    (1, '2024-03-12 08:30:00-03', 138, 90, 105.3, 214.0, 42.0, 148.0, 120.0, 98.0, 83.5, 174.0),
    -- João — visita 2023-09
    (1, '2023-09-05 09:00:00-03', 142, 92,  98.7, 228.5, 40.0, 160.0, 142.0, 97.5, 85.0, 174.0),
    -- Ana Paula — visita 2024-02
    (2, '2024-02-20 10:15:00-03', 142, 95, 182.4, 238.5, 38.0, 162.0, 192.5, 97.0, 79.0, 159.0),
    -- Ana Paula — visita 2023-10
    (2, '2023-10-10 14:30:00-03', 148, 98, 210.0, 245.0, 36.0, 170.0, 195.0, 96.5, 80.5, 159.0),
    -- Carlos — visita 2024-01
    (3, '2024-01-18 11:00:00-03', 118, 76,  89.1, 178.2, 58.0, 102.0,  91.0, 99.0, 79.5, 182.0),
    -- Fernanda — visita 2024-04
    (4, '2024-04-03 09:45:00-03', 110, 70,  92.0, 195.0, 65.0, 112.0,  90.0, 99.0, 61.0, 164.0),
    -- Antônio — visita 2024-03
    (5, '2024-03-28 07:30:00-03', 155,100,  98.0, 252.0, 36.0, 178.0, 190.0, 94.0, 88.0, 172.0),
    -- Antônio — visita 2023-11
    (5, '2023-11-15 08:00:00-03', 162,105,  102.0,258.0, 34.0, 182.0, 210.0, 93.0, 89.5, 172.0);

-- -----------------------------------------------------------------------
-- rastreamento.programa_prevencao
-- -----------------------------------------------------------------------
INSERT INTO rastreamento.programa_prevencao
    (denominacao, orgao_alvo, faixa_etaria_min, faixa_etaria_max,
     periodicidade_meses, metodo_exame, criterio_inclusao, vigente)
VALUES
    ('Rastreamento Câncer Colorretal 50+',
     'intestino_grosso', 50, 75, 12,
     'Sangue oculto nas fezes (FIT) + colonoscopia se positivo',
     'Adultos 50-75 anos sem colonoscopia nos últimos 10 anos', TRUE),
    ('Rastreamento Câncer de Mama 40+',
     'mama', 40, 74, 24,
     'Mamografia bilateral',
     'Mulheres 40-74 anos; anual se risco familiar elevado', TRUE),
    ('Rastreamento Câncer de Próstata PSA 50+',
     'prostata', 50, 70, 12,
     'PSA sérico total + PSA livre',
     'Homens 50-70 anos; a partir de 45 se pai/irmão com Ca de próstata antes de 65', TRUE),
    ('Check-up Cardiovascular Anual',
     'cardiovascular', 35, NULL, 12,
     'Lipidograma, glicemia, PA, ECG de repouso, avaliação de risco pelo Escore de Framingham',
     'Adultos >= 35 anos com ao menos 1 fator de risco cardiovascular', TRUE),
    ('Rastreamento Diabetes Tipo 2',
     'metabolico', 35, NULL, 12,
     'Glicemia de jejum + HbA1c',
     'IMC >= 25 ou histórico familiar de DM2 ou HAS ou dislipidemia', TRUE);

-- -----------------------------------------------------------------------
-- rastreamento.inscricao_programa
-- -----------------------------------------------------------------------
INSERT INTO rastreamento.inscricao_programa
    (beneficiario_id, programa_id, data_ingresso, proxima_realizacao,
     risco_calculado, motivo_ingresso)
VALUES
    -- João: cardiovascular + colorretal
    (1, 4, '2023-09-05', '2025-03-05', 'alto',
     'HAS em tratamento, ex-tabagista, dislipidemia mista, IMC 27.4'),
    (1, 1, '2023-09-05', '2025-09-05', 'intermediario',
     'Faixa etária 50+, sem exame colonoscópico prévio'),
    -- Ana Paula: cardiovascular + mama + diabetes
    (2, 4, '2023-10-10', '2024-10-10', 'muito_alto',
     'DM2 descompensado, obesidade, dislipidemia grave'),
    (2, 2, '2023-10-10', '2024-10-10', 'intermediario',
     'Mulher 55 anos, sem mamografia nos últimos 3 anos'),
    (2, 5, '2023-10-10', '2024-10-10', 'muito_alto',
     'DM2 com HbA1c > 8%, rastreamento de complicações'),
    -- Carlos: próstata
    (3, 3, '2024-01-18', '2025-01-18', 'alto',
     'Pai com câncer de próstata diagnosticado aos 58 anos'),
    -- Fernanda: mama
    (4, 2, '2024-04-03', '2026-04-03', 'baixo',
     'Mulher 45 anos, rastreamento de rotina bienal'),
    -- Antônio: cardiovascular + próstata
    (5, 4, '2023-11-15', '2024-11-15', 'muito_alto',
     'ICFEr, HAS, ex-tabagista, dislipidemia grave, escore de Framingham > 20%'),
    (5, 3, '2023-11-15', '2024-11-15', 'alto',
     'Homem 60 anos, PSA não dosado nos últimos 5 anos');

-- -----------------------------------------------------------------------
-- laboratorio.requisicao_exame
-- -----------------------------------------------------------------------
INSERT INTO laboratorio.requisicao_exame
    (beneficiario_id, medico_requisitante, painel_exames, prioridade,
     inscricao_id, data_emissao, instrucoes_preparo)
VALUES
    -- João — check-up cardiovascular
    (1, 'CRM_PREV001', '["Hemograma completo","Lipidograma completo","Glicemia jejum","Creatinina","Uréia","TSH","ECG repouso"]',
     'rotina', 1, '2024-03-12',
     'Jejum de 12h. Suspender estatinas 48h antes se solicitado pelo médico.'),
    -- Ana Paula — diabetes + cardiovascular
    (2, 'CRM_PREV002', '["HbA1c","Glicemia jejum","Microalbuminúria 24h","Creatinina","Lipidograma","Fundoscopia digital"]',
     'rotina', 3, '2024-02-20',
     'Jejum de 8h para glicemia. Coletar urina de 24h com frasco fornecido pela clínica.'),
    -- Carlos — rastreamento próstata
    (3, 'CRM_PREV001', '["PSA total","PSA livre","Creatinina","Hemograma"]',
     'rotina', 6, '2024-01-18',
     'Abstinência sexual 48h antes. Sem atividade física intensa 24h antes. Toque retal após coleta de sangue.'),
    -- Fernanda — rastreamento mama
    (4, 'CRM_PREV003', '["Mamografia bilateral com tomossíntese","TSH","T4 livre","Hemograma"]',
     'rotina', 7, '2024-04-03',
     'Não usar desodorante, talco ou creme na região das axilas e mamas no dia do exame.'),
    -- Antônio — avaliação cardíaca + laboratorial
    (5, 'CRM_PREV002', '["BNP","Troponina I ultrassensível","Creatinina","Uréia","Lipidograma","Hemograma","Ecocardiograma transtorácico"]',
     'urgente', 8, '2024-03-28',
     'Coleta em repouso. Ecocardiograma agendado para o mesmo dia da consulta.');

-- -----------------------------------------------------------------------
-- laboratorio.resultado_exame
-- -----------------------------------------------------------------------
INSERT INTO laboratorio.resultado_exame
    (requisicao_id, analito, valor_numerico, unidade_medida,
     valor_referencia_min, valor_referencia_max,
     interpretacao, metodo_analitico,
     data_coleta, data_resultado, laboratorio_executante)
VALUES
    -- Req 1 — João
    (1, 'Hemoglobina',        14.2, 'g/dL',      13.5,  17.5, 'normal',          'Citometria de fluxo ABX Pentra',  '2024-03-13 07:30:00-03', '2024-03-13 14:00:00-03', 'Lab CMPD Central'),
    (1, 'Colesterol total',  214.0, 'mg/dL',      NULL,  190.0,'alterado_leve',   'Método enzimático colorimétrico', '2024-03-13 07:30:00-03', '2024-03-13 14:00:00-03', 'Lab CMPD Central'),
    (1, 'LDL calculado',     148.0, 'mg/dL',      NULL,  130.0,'alterado_leve',   'Fórmula de Friedewald',           '2024-03-13 07:30:00-03', '2024-03-13 14:00:00-03', 'Lab CMPD Central'),
    (1, 'HDL',                42.0, 'mg/dL',       40.0,  NULL,'normal',          'Método enzimático colorimétrico', '2024-03-13 07:30:00-03', '2024-03-13 14:00:00-03', 'Lab CMPD Central'),
    (1, 'Glicemia jejum',    105.3, 'mg/dL',       70.0,  99.0,'alterado_leve',   'Hexoquinase enzimático',          '2024-03-13 07:30:00-03', '2024-03-13 14:00:00-03', 'Lab CMPD Central'),
    (1, 'Creatinina',          0.9, 'mg/dL',        0.7,   1.2,'normal',          'Jaffé cinético compensado',       '2024-03-13 07:30:00-03', '2024-03-13 14:00:00-03', 'Lab CMPD Central'),
    -- Req 2 — Ana Paula
    (2, 'HbA1c',               8.4, '%',            NULL,  5.7, 'alterado_moderado','Cromatografia HPLC Tosoh G8',   '2024-02-21 07:00:00-03', '2024-02-21 16:00:00-03', 'Lab CMPD Central'),
    (2, 'Glicemia jejum',    182.4, 'mg/dL',        70.0,  99.0,'critico',         'Hexoquinase enzimático',          '2024-02-21 07:00:00-03', '2024-02-21 16:00:00-03', 'Lab CMPD Central'),
    (2, 'Microalbuminúria',   85.0, 'mg/24h',       NULL,  30.0,'alterado_leve',   'Imunoturbidimetria',              '2024-02-21 07:00:00-03', '2024-02-22 10:00:00-03', 'Lab CMPD Central'),
    (2, 'Creatinina',          0.8, 'mg/dL',         0.5,   1.1,'normal',          'Jaffé cinético compensado',       '2024-02-21 07:00:00-03', '2024-02-21 16:00:00-03', 'Lab CMPD Central'),
    -- Req 3 — Carlos
    (3, 'PSA total',           3.2, 'ng/mL',        NULL,   4.0,'normal',          'Quimioluminescência ECLIA Roche', '2024-01-19 07:15:00-03', '2024-01-19 15:00:00-03', 'Lab CMPD Central'),
    (3, 'PSA livre',           0.8, 'ng/mL',        NULL,  NULL,'normal',          'Quimioluminescência ECLIA Roche', '2024-01-19 07:15:00-03', '2024-01-19 15:00:00-03', 'Lab CMPD Central'),
    (3, 'Índice PSA livre/total', 25.0, '%',        15.0,  NULL,'normal',          'Calculado',                       '2024-01-19 07:15:00-03', '2024-01-19 15:00:00-03', 'Lab CMPD Central'),
    -- Req 5 — Antônio
    (5, 'BNP',               520.0, 'pg/mL',        NULL, 100.0,'critico',         'Quimioluminescência ARCHITECT',   '2024-03-29 07:00:00-03', '2024-03-29 10:00:00-03', 'Lab CMPD Urgência'),
    (5, 'Troponina I',         0.04, 'ng/mL',        NULL,  0.04,'normal',          'ECLIA Elecsys Roche',             '2024-03-29 07:00:00-03', '2024-03-29 10:00:00-03', 'Lab CMPD Urgência'),
    (5, 'Creatinina',          1.6, 'mg/dL',         0.7,   1.2,'alterado_leve',   'Jaffé cinético compensado',       '2024-03-29 07:00:00-03', '2024-03-29 10:00:00-03', 'Lab CMPD Urgência'),
    (5, 'Colesterol total',  252.0, 'mg/dL',        NULL,  190.0,'alterado_moderado','Método enzimático colorimétrico','2024-03-29 07:00:00-03', '2024-03-29 10:00:00-03', 'Lab CMPD Urgência');

-- -----------------------------------------------------------------------
-- laboratorio.achado_imagem
-- -----------------------------------------------------------------------
INSERT INTO laboratorio.achado_imagem
    (requisicao_id, modalidade_imagem, regiao_anatomica,
     classificacao_bi_rads, descricao_radiologica, recomendacao,
     laudado_por, data_laudo_imagem)
VALUES
    -- Req 4 — Fernanda: mamografia bilateral
    (4, 'mamografia', 'mama_bilateral',
     2, 'Mamas de padrão adiposo. Calcificações vasculares bilaterais. Nódulo em mama direita UOE com aspecto benigno (fibroadenoma compatível), estável em relação a exame anterior de 2022. Ausência de microcalcificações suspeitas.',
     'Controle bienal de rotina. Retorno em 24 meses.',
     'CRM_RAD001', '2024-04-05'),
    -- Req 5 — Antônio: ecocardiograma transtorácico
    (5, 'ecocardiograma', 'coracao',
     NULL, 'Ventrículo esquerdo com disfunção sistólica moderada (FE 38% pelo método de Simpson biplano). Dilatação de câmaras esquerdas. Disfunção diastólica grau II. Regurgitação mitral funcional grau I. Pressão sistólica da artéria pulmonar estimada em 42 mmHg.',
     'Otimização do tratamento medicamentoso. Considerar referência para terapia de ressincronização cardíaca (QRS > 150ms a confirmar com ECG 12 derivações). Reavaliação ecocardiográfica em 3 meses.',
     'CRM_RAD002', '2024-03-29'),
    -- Req 1 — João: ECG de repouso (registrado como achado de imagem)
    (1, 'rx', 'torax',
     NULL, 'ECG de repouso 12 derivações: ritmo sinusal, FC 68 bpm, eixo normal, intervalo PR 162ms, QRS 92ms, QTc 412ms. Ausência de alterações isquêmicas agudas ou crônicas. Sem hipertrofia ventricular esquerda por critérios de Sokolow-Lyon.',
     'Exame dentro dos parâmetros normais. Repetir em 12 meses.',
     'CRM_PREV001', '2024-03-13');
