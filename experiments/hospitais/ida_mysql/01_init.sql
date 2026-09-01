-- =============================================================================
-- Hospital A — Instituto de Dermatologia Avançada (IDA)
-- Engine: MySQL 8.0
--
-- No MySQL cada "schema" é um DATABASE separado.
-- O conector Trino MySQL mapeia: catalogo.schema.tabela → database.tabela
--
-- Schemas criados:
--   triagem          — acolhimento e registro fotográfico de lesões
--   anatomopatologia — biópsias e laudos histopatológicos
--   oncologia_pele   — protocolos e evolução terapêutica
--
-- Elo de federação com CMPD: cpf_titular (CHAR 11, sem máscara)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Usuário de aplicação com acesso a todos os schemas do IDA
-- -----------------------------------------------------------------------------
CREATE USER IF NOT EXISTS 'ida_user'@'%' IDENTIFIED BY 'ida_pass';

-- -----------------------------------------------------------------------------
-- Criação dos databases (= schemas no conector Trino MySQL)
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS triagem
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE DATABASE IF NOT EXISTS anatomopatologia
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE DATABASE IF NOT EXISTS oncologia_pele
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

GRANT ALL PRIVILEGES ON triagem.*          TO 'ida_user'@'%';
GRANT ALL PRIVILEGES ON anatomopatologia.* TO 'ida_user'@'%';
GRANT ALL PRIVILEGES ON oncologia_pele.*   TO 'ida_user'@'%';
FLUSH PRIVILEGES;

-- =============================================================================
-- SCHEMA: triagem
-- =============================================================================
USE triagem;

-- Garante idempotência em re-execuções
DROP TABLE IF EXISTS agendamentos_ida;
DROP TABLE IF EXISTS lesoes_fotografadas;
DROP TABLE IF EXISTS ficha_dermato;

-- -----------------------------------------------------------------------
-- Ficha principal do paciente dermatológico
-- -----------------------------------------------------------------------
CREATE TABLE ficha_dermato (
    ficha_id                    INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    cpf_titular                 CHAR(11)     NOT NULL
        COMMENT 'CPF sem máscara — chave de federação com CMPD',
    nome_completo               VARCHAR(150) NOT NULL,
    data_nascimento             DATE         NOT NULL,
    sexo_biologico              CHAR(1)      NOT NULL
        CHECK (sexo_biologico IN ('M','F','I')),
    cor_pele_fitzpatrick        TINYINT      UNSIGNED
        CHECK (cor_pele_fitzpatrick BETWEEN 1 AND 6)
        COMMENT 'Escala de Fitzpatrick I (muito claro) a VI (muito escuro)',
    historico_solar             TEXT
        COMMENT 'Descrição anamnésica de exposição solar acumulada',
    historico_familiar_melanoma BOOLEAN      NOT NULL DEFAULT FALSE,
    data_primeiro_atendimento   DATE         NOT NULL,
    convenio_codigo             VARCHAR(20)
        COMMENT 'Código ANS do convênio',
    CONSTRAINT uq_ficha_cpf UNIQUE (cpf_titular)
) ENGINE=InnoDB COMMENT='Ficha principal do paciente dermatológico';

-- -----------------------------------------------------------------------
-- Registro fotográfico das lesões suspeitas
-- -----------------------------------------------------------------------
CREATE TABLE lesoes_fotografadas (
    lesao_id              INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    ficha_id              INT          NOT NULL,
    localizacao_anatomica VARCHAR(80)  NOT NULL
        COMMENT 'Ex: dorso_superior_esquerdo, face_temporal_direita',
    tamanho_mm            DECIMAL(6,2)
        COMMENT 'Diâmetro maior da lesão em milímetros',
    formato_abcde         JSON
        COMMENT 'Critérios ABCDE: {"assimetria":bool,"borda":"...","cor":"..."}',
    caminho_imagem_s3     TEXT
        COMMENT 'Ex: s3://ida-imagens/lesoes/{lesao_id}.jpg',
    resolucao_dpi         INT UNSIGNED,
    equipamento_usado     VARCHAR(60)
        COMMENT 'Ex: Dermatoscópio Heine Delta 30',
    data_foto             DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    revisado_por          VARCHAR(80)
        COMMENT 'CRM do médico revisor',
    CONSTRAINT fk_lesao_ficha FOREIGN KEY (ficha_id)
        REFERENCES ficha_dermato(ficha_id) ON DELETE CASCADE
) ENGINE=InnoDB COMMENT='Registro fotográfico de lesões suspeitas';

-- -----------------------------------------------------------------------
-- Agenda de consultas dermatológicas
-- -----------------------------------------------------------------------
CREATE TABLE agendamentos_ida (
    agendamento_id    INT         NOT NULL AUTO_INCREMENT PRIMARY KEY,
    ficha_id          INT         NOT NULL,
    medico_crm        VARCHAR(12) NOT NULL,
    modalidade        VARCHAR(30)
        CHECK (modalidade IN ('presencial','teleconsulta')),
    data_hora_marcada DATETIME    NOT NULL,
    sala_numero       TINYINT     UNSIGNED,
    status_agenda     VARCHAR(20) NOT NULL DEFAULT 'pendente'
        CHECK (status_agenda IN ('pendente','confirmado','realizado','cancelado','falta')),
    CONSTRAINT fk_agenda_ficha FOREIGN KEY (ficha_id)
        REFERENCES ficha_dermato(ficha_id)
) ENGINE=InnoDB COMMENT='Agenda de consultas dermatológicas';

CREATE INDEX idx_ficha_cpf    ON ficha_dermato(cpf_titular);
CREATE INDEX idx_lesao_ficha  ON lesoes_fotografadas(ficha_id);
CREATE INDEX idx_agenda_data  ON agendamentos_ida(data_hora_marcada);

-- =============================================================================
-- SCHEMA: anatomopatologia
-- =============================================================================
USE anatomopatologia;

DROP TABLE IF EXISTS imunohistoquimica;
DROP TABLE IF EXISTS laudo_histopatologico;
DROP TABLE IF EXISTS biopsia_solicitacao;

-- -----------------------------------------------------------------------
-- Solicitação de biópsia emitida pelo dermatologista
-- -----------------------------------------------------------------------
CREATE TABLE biopsia_solicitacao (
    solicitacao_id         INT         NOT NULL AUTO_INCREMENT PRIMARY KEY,
    lesao_id               INT         NOT NULL
        COMMENT 'ref → triagem.lesoes_fotografadas(lesao_id)',
    medico_solicitante_crm VARCHAR(12) NOT NULL,
    tecnica_biopsia        VARCHAR(40)
        CHECK (tecnica_biopsia IN ('punch','excisional','incisional','shave','curetagem')),
    urgencia               VARCHAR(10) NOT NULL DEFAULT 'eletiva'
        CHECK (urgencia IN ('eletiva','urgente','emergencial')),
    data_solicitacao       DATE        NOT NULL,
    observacoes_clinicas   TEXT
) ENGINE=InnoDB COMMENT='Solicitações de biópsia de lesões cutâneas';

-- -----------------------------------------------------------------------
-- Laudo histopatológico emitido pelo patologista
-- -----------------------------------------------------------------------
CREATE TABLE laudo_histopatologico (
    laudo_id             INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    solicitacao_id       INT          NOT NULL,
    patologista_crm      VARCHAR(12)  NOT NULL,
    classificacao_who    VARCHAR(80)
        COMMENT 'Classificação WHO de tumores cutâneos',
    espessura_breslow_mm DECIMAL(5,3)
        COMMENT 'Índice de Breslow em milímetros',
    nivel_clark          TINYINT
        CHECK (nivel_clark BETWEEN 1 AND 5),
    margem_livre         BOOLEAN,
    indice_mitose        DECIMAL(4,1)
        COMMENT 'Mitoses por mm²',
    estadiamento_tnm     VARCHAR(10)
        COMMENT 'Ex: T2aN0M0',
    codigo_cid10         CHAR(4)      NOT NULL
        COMMENT 'Ex: C433 = melanoma de tronco, C440 = CBC de lábio',
    data_laudo           DATE         NOT NULL,
    numero_protocolo     VARCHAR(30)  NOT NULL,
    CONSTRAINT fk_laudo_solic      FOREIGN KEY (solicitacao_id)
        REFERENCES biopsia_solicitacao(solicitacao_id),
    CONSTRAINT uq_numero_protocolo UNIQUE (numero_protocolo)
) ENGINE=InnoDB COMMENT='Laudos histopatológicos pós-biópsia';

-- -----------------------------------------------------------------------
-- Marcadores moleculares (imunoistoquímica complementar)
-- -----------------------------------------------------------------------
CREATE TABLE imunohistoquimica (
    marcador_id        INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    laudo_id           INT          NOT NULL,
    anticorpo          VARCHAR(40)  NOT NULL
        COMMENT 'Ex: S-100, HMB-45, MelanA, BRAF V600E, Ki-67',
    resultado          VARCHAR(20)
        CHECK (resultado IN ('positivo','negativo','indeterminado')),
    intensidade        VARCHAR(10)
        CHECK (intensidade IN ('fraca','moderada','forte')),
    percentual_celulas DECIMAL(5,2)
        CHECK (percentual_celulas BETWEEN 0 AND 100),
    CONSTRAINT fk_marcador_laudo FOREIGN KEY (laudo_id)
        REFERENCES laudo_histopatologico(laudo_id)
) ENGINE=InnoDB COMMENT='Marcadores por imuno-histoquímica';

CREATE INDEX idx_biopsia_lesao ON biopsia_solicitacao(lesao_id);
CREATE INDEX idx_laudo_cid     ON laudo_histopatologico(codigo_cid10);
CREATE INDEX idx_laudo_solic   ON laudo_histopatologico(solicitacao_id);
CREATE INDEX idx_ihq_laudo     ON imunohistoquimica(laudo_id);

-- =============================================================================
-- SCHEMA: oncologia_pele
-- =============================================================================
USE oncologia_pele;

DROP TABLE IF EXISTS evolucao_clinica;
DROP TABLE IF EXISTS protocolo_tratamento;

-- -----------------------------------------------------------------------
-- Protocolo terapêutico oncológico
-- -----------------------------------------------------------------------
CREATE TABLE protocolo_tratamento (
    protocolo_id          INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    laudo_id              INT          NOT NULL
        COMMENT 'ref → anatomopatologia.laudo_histopatologico(laudo_id)',
    oncologista_crm       VARCHAR(12)  NOT NULL,
    modalidade_terapia    VARCHAR(50)
        CHECK (modalidade_terapia IN (
            'cirurgia_ampliada','radioterapia','imunoterapia',
            'quimioterapia','terapia_alvo','vigilancia_ativa','combinado')),
    medicamento_principal VARCHAR(100)
        COMMENT 'Ex: Pembrolizumab 200mg, Vemurafenib 960mg',
    dose_mg               DECIMAL(8,3),
    ciclos_previstos      SMALLINT,
    intervalo_dias        SMALLINT
        COMMENT 'Intervalo em dias entre ciclos',
    data_inicio           DATE         NOT NULL,
    data_fim_prevista     DATE,
    status_protocolo      VARCHAR(20)  NOT NULL DEFAULT 'ativo'
        CHECK (status_protocolo IN ('ativo','suspenso','concluido','abandonado'))
) ENGINE=InnoDB COMMENT='Protocolos terapêuticos oncológicos';

-- -----------------------------------------------------------------------
-- Registro de evolução clínica a cada consulta de retorno
-- -----------------------------------------------------------------------
CREATE TABLE evolucao_clinica (
    evolucao_id       INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    protocolo_id      INT          NOT NULL,
    data_consulta     DATE         NOT NULL,
    ciclo_atual       TINYINT      UNSIGNED,
    resposta_ecog     TINYINT
        CHECK (resposta_ecog BETWEEN 0 AND 5)
        COMMENT 'ECOG Performance Status: 0=assintomático, 5=óbito',
    dimensao_lesao_mm DECIMAL(6,2)
        COMMENT 'Tamanho atual da lesão residual/metástase em mm',
    toxicidade_grau   TINYINT
        CHECK (toxicidade_grau BETWEEN 0 AND 4)
        COMMENT 'CTCAE grau de toxicidade: 0=sem, 4=risco de vida',
    ajuste_dose       BOOLEAN      NOT NULL DEFAULT FALSE,
    anotacao_medica   TEXT,
    CONSTRAINT fk_evolucao_protocolo FOREIGN KEY (protocolo_id)
        REFERENCES protocolo_tratamento(protocolo_id)
) ENGINE=InnoDB COMMENT='Evoluções clínicas durante o tratamento';

CREATE INDEX idx_protocolo_laudo    ON protocolo_tratamento(laudo_id);
CREATE INDEX idx_evolucao_data      ON evolucao_clinica(data_consulta);
CREATE INDEX idx_evolucao_protocolo ON evolucao_clinica(protocolo_id);


-- =============================================================================
-- SEED DATA — dados sintéticos realistas
-- Pacientes compartilhados com CMPD (mesmo CPF):
--   12345678901 | João Carlos Ferreira Machado
--   98765432100 | Ana Paula Santos Lima
--   55544433322 | Carlos Eduardo Oliveira Neto
-- Pacientes exclusivos do IDA:
--   11122233344 | Mariana Conceição Pereira
--   99988877766 | Roberto Alves Mendonça
-- =============================================================================

-- -------------------------
-- triagem.ficha_dermato
-- -------------------------
USE triagem;

INSERT INTO ficha_dermato
    (cpf_titular, nome_completo, data_nascimento, sexo_biologico,
     cor_pele_fitzpatrick, historico_solar, historico_familiar_melanoma,
     data_primeiro_atendimento, convenio_codigo)
VALUES
    ('12345678901', 'João Carlos Ferreira Machado', '1975-03-15', 'M',
     2, 'Trabalhou em atividade rural por 15 anos, exposição intensa sem protetor', TRUE,
     '2023-08-10', 'ANS12345'),
    ('98765432100', 'Ana Paula Santos Lima', '1968-07-22', 'F',
     3, 'Histórico de bronzeamento artificial até 2010', FALSE,
     '2023-11-03', 'ANS67890'),
    ('55544433322', 'Carlos Eduardo Oliveira Neto', '1982-11-08', 'M',
     1, 'Fototipos claro, múltiplos episódios de queimadura solar na infância', TRUE,
     '2024-01-20', 'ANS11223'),
    ('11122233344', 'Mariana Conceição Pereira', '1990-05-30', 'F',
     4, 'Exposição solar moderada, uso regular de protetor solar desde 2015', FALSE,
     '2024-03-05', NULL),
    ('99988877766', 'Roberto Alves Mendonça', '1955-12-01', 'M',
     2, 'Trabalhador da construção civil, 30 anos de exposição solar intensa', TRUE,
     '2023-06-18', 'ANS44556');

-- -------------------------
-- triagem.lesoes_fotografadas
-- -------------------------
INSERT INTO lesoes_fotografadas
    (ficha_id, localizacao_anatomica, tamanho_mm, formato_abcde,
     caminho_imagem_s3, resolucao_dpi, equipamento_usado, data_foto, revisado_por)
VALUES
    (1, 'escapula_direita', 14.3,
     '{"assimetria":true,"borda":"irregular","cor":"multicolorida","diametro_mm":14.3,"evolucao":"crescimento_3meses"}',
     's3://ida-imagens/lesoes/1.jpg', 1200, 'Dermatoscópio Heine Delta 30', '2023-08-10 09:30:00', 'CRM123456'),
    (1, 'antebraco_esquerdo', 8.2,
     '{"assimetria":false,"borda":"regular","cor":"marrom_uniforme","diametro_mm":8.2,"evolucao":"estavel"}',
     's3://ida-imagens/lesoes/2.jpg', 1200, 'Dermatoscópio Heine Delta 30', '2023-08-10 09:45:00', 'CRM123456'),
    (2, 'dorso_inferior_medio', 12.1,
     '{"assimetria":true,"borda":"irregular","cor":"negra_avermelhada","diametro_mm":12.1,"evolucao":"crescimento_rapido"}',
     's3://ida-imagens/lesoes/3.jpg', 1200, 'Dermatoscópio FotoFinder Medicam 1000', '2023-11-03 14:15:00', 'CRM789012'),
    (3, 'face_temporal_direita', 9.5,
     '{"assimetria":false,"borda":"irregular_perolada","cor":"rosea","diametro_mm":9.5,"evolucao":"sangramento_ocasional"}',
     's3://ida-imagens/lesoes/4.jpg', 1200, 'Dermatoscópio Dermoscopy DL100', '2024-01-20 10:00:00', 'CRM345678'),
    (4, 'panturrilha_esquerda', 6.8,
     '{"assimetria":false,"borda":"regular","cor":"marrom_claro","diametro_mm":6.8,"evolucao":"estavel"}',
     's3://ida-imagens/lesoes/5.jpg', 600, 'Dermatoscópio Heine Delta 30', '2024-03-05 11:30:00', 'CRM123456'),
    (5, 'tronco_anterior_superior', 18.7,
     '{"assimetria":true,"borda":"irregular","cor":"marrom_escura","diametro_mm":18.7,"evolucao":"crescimento_6meses"}',
     's3://ida-imagens/lesoes/6.jpg', 1200, 'Dermatoscópio FotoFinder Medicam 1000', '2023-06-18 15:00:00', 'CRM789012');

-- -------------------------
-- triagem.agendamentos_ida
-- -------------------------
INSERT INTO agendamentos_ida
    (ficha_id, medico_crm, modalidade, data_hora_marcada, sala_numero, status_agenda)
VALUES
    (1, 'CRM123456', 'presencial', '2023-08-10 09:00:00', 3, 'realizado'),
    (1, 'CRM123456', 'presencial', '2023-09-14 09:00:00', 3, 'realizado'),
    (2, 'CRM789012', 'presencial', '2023-11-03 14:00:00', 5, 'realizado'),
    (3, 'CRM345678', 'presencial', '2024-01-20 10:00:00', 2, 'realizado'),
    (4, 'CRM123456', 'teleconsulta', '2024-03-05 11:00:00', NULL, 'realizado'),
    (5, 'CRM789012', 'presencial', '2023-06-18 14:30:00', 5, 'realizado'),
    (1, 'CRM123456', 'presencial', '2024-02-15 09:00:00', 3, 'realizado'),
    (2, 'CRM789012', 'presencial', '2024-01-08 14:00:00', 5, 'realizado');

-- -----------------------------------------------------------------------
-- anatomopatologia.biopsia_solicitacao
-- -----------------------------------------------------------------------
USE anatomopatologia;

INSERT INTO biopsia_solicitacao
    (lesao_id, medico_solicitante_crm, tecnica_biopsia, urgencia, data_solicitacao, observacoes_clinicas)
VALUES
    (1, 'CRM123456', 'excisional', 'eletiva',   '2023-09-14', 'Lesão com critérios dermatoscópicos de melanoma — suspeita alta'),
    (3, 'CRM789012', 'excisional', 'urgente',   '2023-11-03', 'Crescimento rápido em 6 semanas, paciente com histórico de bronzeamento artificial'),
    (4, 'CRM345678', 'punch',      'eletiva',   '2024-01-20', 'Bordas peroladas típicas de CBC, punch 4mm para confirmação'),
    (6, 'CRM789012', 'excisional', 'urgente',   '2023-06-18', 'Lesão > 15mm com assimetria marcada, excisão imediata recomendada'),
    (2, 'CRM123456', 'shave',      'eletiva',   '2024-02-15', 'Lesão benigna aparente, shave para confirmação histológica');

-- -----------------------------------------------------------------------
-- anatomopatologia.laudo_histopatologico
-- -----------------------------------------------------------------------
INSERT INTO laudo_histopatologico
    (solicitacao_id, patologista_crm, classificacao_who, espessura_breslow_mm,
     nivel_clark, margem_livre, indice_mitose, estadiamento_tnm,
     codigo_cid10, data_laudo, numero_protocolo)
VALUES
    (1, 'CRM_PAT001', 'Melanoma maligno, extensão superficial',
     1.82, 3, TRUE,  2.1, 'T2aN0M0', 'C435', '2023-09-28', 'PROT-IDA-2023-0147'),
    (2, 'CRM_PAT002', 'Melanoma maligno nodular',
     3.50, 4, FALSE, 6.8, 'T3bN1M0', 'C439', '2023-11-17', 'PROT-IDA-2023-0189'),
    (3, 'CRM_PAT001', 'Carcinoma basocelular nodular',
     NULL, 2, TRUE,  0.0, NULL,      'C440', '2024-02-05', 'PROT-IDA-2024-0022'),
    (4, 'CRM_PAT002', 'Melanoma maligno, extensão superficial',
     2.10, 3, FALSE, 3.2, 'T2bN0M0', 'C435', '2023-07-03', 'PROT-IDA-2023-0098'),
    (5, 'CRM_PAT001', 'Nevo melanocítico composto — benigno',
     NULL, 1, TRUE,  0.0, NULL,      'D229', '2024-02-28', 'PROT-IDA-2024-0035');

-- -----------------------------------------------------------------------
-- anatomopatologia.imunohistoquimica
-- -----------------------------------------------------------------------
INSERT INTO imunohistoquimica
    (laudo_id, anticorpo, resultado, intensidade, percentual_celulas)
VALUES
    -- Laudo 1 (João — melanoma T2a)
    (1, 'S-100',     'positivo',      'forte',    98.0),
    (1, 'HMB-45',    'positivo',      'moderada', 75.0),
    (1, 'MelanA',    'positivo',      'forte',    92.0),
    (1, 'Ki-67',     'positivo',      'moderada', 18.0),
    (1, 'BRAF V600E','negativo',      NULL,       NULL),
    -- Laudo 2 (Ana Paula — melanoma nodular T3b)
    (2, 'S-100',     'positivo',      'forte',    100.0),
    (2, 'HMB-45',    'positivo',      'forte',    88.0),
    (2, 'BRAF V600E','positivo',      'forte',    95.0),
    (2, 'Ki-67',     'positivo',      'forte',    42.0),
    -- Laudo 4 (Roberto — melanoma T2b)
    (4, 'S-100',     'positivo',      'forte',    97.0),
    (4, 'MelanA',    'positivo',      'moderada', 80.0),
    (4, 'BRAF V600E','indeterminado', NULL,       NULL);

-- -----------------------------------------------------------------------
-- oncologia_pele.protocolo_tratamento
-- -----------------------------------------------------------------------
USE oncologia_pele;

INSERT INTO protocolo_tratamento
    (laudo_id, oncologista_crm, modalidade_terapia, medicamento_principal,
     dose_mg, ciclos_previstos, intervalo_dias,
     data_inicio, data_fim_prevista, status_protocolo)
VALUES
    -- João (laudo 1 — T2aN0M0): imunoterapia adjuvante
    (1, 'CRM_ONC001', 'imunoterapia',   'Pembrolizumab',  200.000, 18, 21,
     '2023-10-15', '2025-03-30', 'ativo'),
    -- Ana Paula (laudo 2 — T3bN1M0): terapia alvo + cirurgia
    (2, 'CRM_ONC002', 'combinado',      'Vemurafenib',    960.000,  8, 14,
     '2023-12-01', '2024-07-30', 'concluido'),
    -- Roberto (laudo 4 — T2bN0M0): cirurgia ampliada + vigilância
    (4, 'CRM_ONC001', 'cirurgia_ampliada', NULL,          NULL,    NULL, NULL,
     '2023-07-20', '2023-07-20', 'concluido');

-- -----------------------------------------------------------------------
-- oncologia_pele.evolucao_clinica
-- -----------------------------------------------------------------------
INSERT INTO evolucao_clinica
    (protocolo_id, data_consulta, ciclo_atual, resposta_ecog,
     dimensao_lesao_mm, toxicidade_grau, ajuste_dose, anotacao_medica)
VALUES
    -- João — protocolo 1, Pembrolizumab
    (1, '2023-10-15', 1,  1, 14.3, 0, FALSE, 'Início de protocolo. Paciente tolerando bem.'),
    (1, '2023-11-05', 2,  1, 12.0, 1, FALSE, 'Rash cutâneo grau 1 transitório. Mantida dose.'),
    (1, '2023-11-26', 3,  1,  9.8, 1, FALSE, 'Resposta parcial confirmada. RECIST -32%.'),
    (1, '2024-01-15', 6,  0,  5.2, 0, FALSE, 'Resposta completa por imagem. Aguardar PET-CT.'),
    (1, '2024-04-10', 12, 0,  0.0, 0, FALSE, 'Remissão mantida. PET-CT negativo.'),
    -- Ana Paula — protocolo 2, Vemurafenib
    (2, '2023-12-01', 1,  2, 28.5, 1, FALSE, 'Início. Fotossensibilidade grau 1. Protetor solar reforçado.'),
    (2, '2023-12-15', 2,  2, 22.0, 2, TRUE,  'Elevação de transaminases grau 2. Dose reduzida para 720mg.'),
    (2, '2024-01-12', 4,  1, 14.0, 1, FALSE, 'Melhora hepática. Retomada dose plena.'),
    (2, '2024-05-20', 8,  1,  3.5, 1, FALSE, 'Resposta completa. Protocolo encerrado. Vigilância semestral.'),
    -- Roberto — protocolo 3, pós-cirurgia
    (3, '2023-08-20', NULL, 0, 0.0, 0, FALSE, 'Revisão pós-cirúrgica. Cicatriz em boas condições. Margens re-excisadas.');
