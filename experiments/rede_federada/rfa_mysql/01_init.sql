-- =============================================================================
-- RFA — Rede de Farmácias e Adesão
-- Engine: MySQL 8.0
--
-- No MySQL cada "schema" é um DATABASE separado.
-- O conector Trino MySQL mapeia: catalogo.schema.tabela → database.tabela
--
-- Schemas (databases):
--   dispensacao        — cliente, receita médica e itens dispensados
--   farmacovigilancia  — eventos adversos e interações medicamentosas
--   adesao_terapeutica — programas de adesão e acompanhamento mensal
--
-- Elo de federação: cpf_cliente (CHAR 11, sem máscara)
-- Topologia: RFA compartilha CPFs com CDI; NÃO compartilha com REH.
--
-- Schema + seed NOMINAL. Volume de 500k em seeds/rfa_bulk.sql.
-- =============================================================================

CREATE USER IF NOT EXISTS 'rfa_user'@'%' IDENTIFIED BY 'rfa_pass';

CREATE DATABASE IF NOT EXISTS dispensacao        CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS farmacovigilancia  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS adesao_terapeutica CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

GRANT ALL PRIVILEGES ON dispensacao.*        TO 'rfa_user'@'%';
GRANT ALL PRIVILEGES ON farmacovigilancia.*  TO 'rfa_user'@'%';
GRANT ALL PRIVILEGES ON adesao_terapeutica.* TO 'rfa_user'@'%';
FLUSH PRIVILEGES;

-- =============================================================================
-- DATABASE: dispensacao
-- =============================================================================
USE dispensacao;

DROP TABLE IF EXISTS item_dispensado;
DROP TABLE IF EXISTS receita_medica;
DROP TABLE IF EXISTS cliente_farmacia;

CREATE TABLE cliente_farmacia (
    cliente_id      INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    cpf_cliente     CHAR(11)     NOT NULL
        COMMENT 'CPF sem máscara — chave de federação (CDI↔RFA)',
    nome_completo   VARCHAR(150) NOT NULL,
    data_nascimento DATE         NOT NULL,
    sexo_biologico  CHAR(1)      CHECK (sexo_biologico IN ('M','F','I')),
    telefone        VARCHAR(15),
    municipio       VARCHAR(80),
    uf_sigla        CHAR(2),
    aceita_generico BOOLEAN      NOT NULL DEFAULT TRUE,
    cadastrado_em   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_rfa_cpf UNIQUE (cpf_cliente)
) ENGINE=InnoDB COMMENT='Cadastro central do cliente da rede de farmácias';

CREATE TABLE receita_medica (
    receita_id        INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    cliente_id        INT          NOT NULL,
    medico_crm        VARCHAR(12)  NOT NULL,
    tipo_receita      VARCHAR(15)  NOT NULL
        CHECK (tipo_receita IN ('comum','controlada','antimicrobiano')),
    data_prescricao   DATE         NOT NULL,
    validade          DATE,
    cid10_associado   CHAR(4),
    origem_atendimento VARCHAR(40)
        COMMENT 'Ex: ambulatorial, alta hospitalar, telemedicina',
    CONSTRAINT fk_receita_cliente FOREIGN KEY (cliente_id)
        REFERENCES cliente_farmacia(cliente_id) ON DELETE CASCADE
) ENGINE=InnoDB COMMENT='Receitas médicas apresentadas na farmácia';

CREATE TABLE item_dispensado (
    item_id          BIGINT        NOT NULL AUTO_INCREMENT PRIMARY KEY,
    receita_id       INT           NOT NULL,
    medicamento      VARCHAR(120)  NOT NULL,
    principio_ativo  VARCHAR(120),
    apresentacao     VARCHAR(60)
        COMMENT 'Ex: comprimido 50mg, frasco 100ml',
    quantidade       INT           NOT NULL,
    lote             VARCHAR(30),
    preco_unitario   DECIMAL(10,2),
    valor_total      DECIMAL(10,2),
    data_retirada    DATETIME      NOT NULL,
    CONSTRAINT fk_item_receita FOREIGN KEY (receita_id)
        REFERENCES receita_medica(receita_id) ON DELETE CASCADE
) ENGINE=InnoDB COMMENT='Itens efetivamente dispensados por receita';

CREATE INDEX idx_rfa_cli_cpf   ON cliente_farmacia(cpf_cliente);
CREATE INDEX idx_rfa_rec_cli   ON receita_medica(cliente_id);
CREATE INDEX idx_rfa_rec_data  ON receita_medica(data_prescricao);
CREATE INDEX idx_rfa_item_rec  ON item_dispensado(receita_id);

-- =============================================================================
-- DATABASE: farmacovigilancia
-- =============================================================================
USE farmacovigilancia;

DROP TABLE IF EXISTS interacao_medicamentosa;
DROP TABLE IF EXISTS evento_adverso;

CREATE TABLE evento_adverso (
    evento_id           BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    cliente_id          INT          NOT NULL
        COMMENT 'ref → dispensacao.cliente_farmacia(cliente_id)',
    medicamento_suspeito VARCHAR(120) NOT NULL,
    descricao_evento    TEXT,
    gravidade           VARCHAR(10)
        CHECK (gravidade IN ('leve','moderado','grave')),
    desfecho            VARCHAR(20)
        CHECK (desfecho IN ('recuperado','em_recuperacao','sequela','obito','desconhecido')),
    notificado_anvisa   BOOLEAN      NOT NULL DEFAULT FALSE,
    data_evento         DATE         NOT NULL
) ENGINE=InnoDB COMMENT='Eventos adversos a medicamentos (farmacovigilância)';

CREATE TABLE interacao_medicamentosa (
    interacao_id    BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    cliente_id      INT          NOT NULL
        COMMENT 'ref → dispensacao.cliente_farmacia(cliente_id)',
    farmaco_a       VARCHAR(120) NOT NULL,
    farmaco_b       VARCHAR(120) NOT NULL,
    severidade      VARCHAR(12)
        CHECK (severidade IN ('menor','moderada','maior','contraindicada')),
    mecanismo       VARCHAR(200),
    recomendacao    TEXT,
    detectada_em    DATE         NOT NULL
) ENGINE=InnoDB COMMENT='Interações medicamentosas detectadas no perfil do cliente';

CREATE INDEX idx_rfa_ev_cli   ON evento_adverso(cliente_id);
CREATE INDEX idx_rfa_ev_grav  ON evento_adverso(gravidade);
CREATE INDEX idx_rfa_int_cli  ON interacao_medicamentosa(cliente_id);

-- =============================================================================
-- DATABASE: adesao_terapeutica
-- =============================================================================
USE adesao_terapeutica;

DROP TABLE IF EXISTS acompanhamento_adesao;
DROP TABLE IF EXISTS programa_adesao;

CREATE TABLE programa_adesao (
    programa_id      INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    nome_programa    VARCHAR(120) NOT NULL UNIQUE,
    doenca_alvo      VARCHAR(40)  NOT NULL
        COMMENT 'Ex: HAS, DM2, dislipidemia, anticoagulacao',
    meta_aderencia_pct DECIMAL(5,2) NOT NULL DEFAULT 80.00,
    descricao        TEXT,
    vigente          BOOLEAN      NOT NULL DEFAULT TRUE
) ENGINE=InnoDB COMMENT='Catálogo de programas de adesão terapêutica';

CREATE TABLE acompanhamento_adesao (
    acompanhamento_id BIGINT      NOT NULL AUTO_INCREMENT PRIMARY KEY,
    cliente_id        INT         NOT NULL
        COMMENT 'ref → dispensacao.cliente_farmacia(cliente_id)',
    programa_id       INT         NOT NULL,
    mes_referencia    DATE        NOT NULL,
    doses_previstas   SMALLINT    NOT NULL,
    doses_retiradas   SMALLINT    NOT NULL,
    indice_adesao_pct DECIMAL(5,2),
    status_adesao     VARCHAR(15)
        CHECK (status_adesao IN ('aderente','parcial','abandono')),
    CONSTRAINT fk_acomp_programa FOREIGN KEY (programa_id)
        REFERENCES programa_adesao(programa_id)
) ENGINE=InnoDB COMMENT='Acompanhamento mensal de adesão por cliente/programa';

CREATE INDEX idx_rfa_ac_cli   ON acompanhamento_adesao(cliente_id);
CREATE INDEX idx_rfa_ac_prog  ON acompanhamento_adesao(programa_id);
CREATE INDEX idx_rfa_ac_mes   ON acompanhamento_adesao(mes_referencia);

-- =============================================================================
-- SEED NOMINAL. IDs em banda alta (900000001+).
--
-- CPFs compartilhados com CDI : 22222000001, 22222000002
-- CPFs exclusivos do RFA      : 55555000001, 55555000002
-- (NENHUM CPF compartilhado com REH — respeita a topologia em cadeia)
-- =============================================================================
USE dispensacao;

INSERT INTO cliente_farmacia
    (cliente_id, cpf_cliente, nome_completo, data_nascimento, sexo_biologico, telefone, municipio, uf_sigla, aceita_generico)
VALUES
    (900000001, '22222000001', 'Otávio Camargo Pinto',   '1978-03-22', 'M', '61999220001', 'Brasília',  'DF', TRUE),
    (900000002, '22222000002', 'Renata Vasconcelos Sá',  '1990-11-14', 'F', '61999220002', 'Taguatinga','DF', TRUE),
    (900000003, '55555000001', 'Iolanda Furtado Bastos', '1955-01-09', 'F', '61999550001', 'Brasília',  'DF', FALSE),
    (900000004, '55555000002', 'Wagner Tibúrcio Rocha',  '1983-07-27', 'M', '61999550002', 'Gama',      'DF', TRUE);

INSERT INTO receita_medica
    (receita_id, cliente_id, medico_crm, tipo_receita, data_prescricao, validade, cid10_associado, origem_atendimento)
VALUES
    (900000001, 900000001, 'CRM_URO11', 'comum',         '2024-05-21', '2024-11-21', 'N40', 'ambulatorial'),
    (900000002, 900000002, 'CRM_MAS22', 'comum',         '2024-06-03', '2024-12-03', 'C50', 'alta hospitalar'),
    (900000003, 900000003, 'CRM_CAR55', 'controlada',    '2024-04-10', '2024-07-10', 'I48', 'ambulatorial'),
    (900000004, 900000004, 'CRM_END66', 'antimicrobiano','2024-05-15', '2024-05-25', 'L08', 'telemedicina');

INSERT INTO item_dispensado
    (receita_id, medicamento, principio_ativo, apresentacao, quantidade, lote, preco_unitario, valor_total, data_retirada)
VALUES
    (900000001, 'Tansulosina', 'cloridrato de tansulosina', 'cápsula 0,4mg', 30, 'L24A001', 1.20, 36.00, '2024-05-21 16:30:00'),
    (900000003, 'Varfarina',   'varfarina sódica',          'comprimido 5mg', 30, 'L24B055', 0.45, 13.50, '2024-04-10 10:00:00'),
    (900000003, 'Marevan',     'varfarina sódica',          'comprimido 2,5mg',30,'L24B056', 0.40, 12.00, '2024-04-10 10:00:00'),
    (900000004, 'Cefalexina',  'cefalexina',                'comprimido 500mg',21,'L24C100', 0.90, 18.90, '2024-05-15 18:00:00');

USE farmacovigilancia;

INSERT INTO evento_adverso
    (cliente_id, medicamento_suspeito, descricao_evento, gravidade, desfecho, notificado_anvisa, data_evento)
VALUES
    (900000003, 'Varfarina', 'Epistaxe recorrente e equimoses — RNI 4.8 (supraterapêutico).', 'moderado', 'recuperado', TRUE, '2024-04-28'),
    (900000004, 'Cefalexina','Rash maculopapular difuso após 3ª dose.', 'leve', 'recuperado', FALSE, '2024-05-17');

INSERT INTO interacao_medicamentosa
    (cliente_id, farmaco_a, farmaco_b, severidade, mecanismo, recomendacao, detectada_em)
VALUES
    (900000003, 'Varfarina', 'AAS', 'maior', 'Sinergismo anticoagulante/antiagregante — risco hemorrágico aumentado.', 'Monitorar RNI; evitar associação se possível.', '2024-04-10');

USE adesao_terapeutica;

INSERT INTO programa_adesao (programa_id, nome_programa, doenca_alvo, meta_aderencia_pct, descricao) VALUES
    (900000001, 'Adesão Anticoagulação Oral', 'anticoagulacao', 85.00, 'Acompanhamento de pacientes em uso de varfarina/DOACs'),
    (900000002, 'Controle de Hipertensão',    'HAS',            80.00, 'Reforço de adesão ao tratamento anti-hipertensivo');

INSERT INTO acompanhamento_adesao
    (cliente_id, programa_id, mes_referencia, doses_previstas, doses_retiradas, indice_adesao_pct, status_adesao)
VALUES
    (900000003, 900000001, '2024-04-01', 30, 30, 100.00, 'aderente'),
    (900000003, 900000001, '2024-05-01', 31, 22,  70.97, 'parcial');

ALTER TABLE dispensacao.cliente_farmacia AUTO_INCREMENT       = 900000100;
ALTER TABLE dispensacao.receita_medica   AUTO_INCREMENT       = 900000100;
ALTER TABLE adesao_terapeutica.programa_adesao AUTO_INCREMENT = 900000100;
