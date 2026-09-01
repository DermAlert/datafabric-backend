-- ============================================================
-- IDA — MySQL: bulk seed de 1000 novos pacientes
-- CPF range: 00000001001..00000002000
-- CPFs 00000001001..00000001700 aparecem também no CMPD (federados)
-- CPFs 00000001701..00000002000 são exclusivos do IDA
-- ============================================================

USE triagem;

SET FOREIGN_KEY_CHECKS = 0;
SET UNIQUE_CHECKS     = 0;
SET SQL_MODE = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- --------------------------------------------------------
-- Gerador de sequência 1001..2000
-- --------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS t_seq;
CREATE TEMPORARY TABLE t_seq (n INT NOT NULL PRIMARY KEY);

INSERT INTO t_seq (n)
SELECT a.d + b.d*10 + c.d*100 + 1001
FROM
  (SELECT 0 d UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
   UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a
  CROSS JOIN
  (SELECT 0 d UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
   UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b
  CROSS JOIN
  (SELECT 0 d UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
   UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c
WHERE a.d + b.d*10 + c.d*100 + 1001 <= 2000;

-- ============================================================
-- SCHEMA: triagem
-- ============================================================
USE triagem;

-- --------------------------------------------------------
-- 1000 fichas novas
-- --------------------------------------------------------
INSERT INTO ficha_dermato
  (cpf_titular, nome_completo, data_nascimento, sexo_biologico,
   cor_pele_fitzpatrick, historico_solar, historico_familiar_melanoma,
   data_primeiro_atendimento, convenio_codigo)
SELECT
  LPAD(n, 11, '0'),
  CONCAT(
    ELT(1+(n%25),
      'João','Maria','Carlos','Ana','Pedro','Fernanda','Roberto','Juliana',
      'Ricardo','Patrícia','Eduardo','Camila','Marcos','Beatriz','Lucas',
      'Amanda','Bruno','Larissa','Diego','Vanessa','Rafael','Letícia',
      'André','Gabriela','Paulo'),
    ' ',
    ELT(1+((n*7)%25),
      'Silva','Santos','Oliveira','Pereira','Costa','Rodrigues','Alves',
      'Nascimento','Lima','Araújo','Ferreira','Carvalho','Melo','Barbosa',
      'Ribeiro','Castro','Monteiro','Gomes','Souza','Freitas','Moreira',
      'Xavier','Cardoso','Mendes','Correia')
  ),
  DATE_ADD('1945-01-01', INTERVAL ((n*13) % 21900) DAY),
  IF(n%2=0,'F','M'),
  1 + (n%6),
  ELT(1+(n%4),
    'Exposição solar intensa por trabalho ao ar livre',
    'Histórico de queimaduras solares na infância',
    'Exposição moderada sem protetor solar regular',
    'Uso consistente de protetor solar desde jovem'),
  (n%5 = 0),
  DATE_ADD('2020-01-01', INTERVAL ((n*11) % 1460) DAY),
  IF(n%7=0, NULL, CONCAT('ANS', LPAD(n%9999, 5, '0')))
FROM t_seq;

-- --------------------------------------------------------
-- Lesão principal de cada paciente (~1000 lesões)
-- --------------------------------------------------------
INSERT INTO lesoes_fotografadas
  (ficha_id, localizacao_anatomica, tamanho_mm, formato_abcde,
   caminho_imagem_s3, resolucao_dpi, equipamento_usado, data_foto, revisado_por)
SELECT
  f.ficha_id,
  ELT(1+(f.ficha_id%10),
    'escapula_direita','escapula_esquerda','dorso_superior_esquerdo',
    'dorso_inferior_medio','tronco_anterior_superior','braco_direito',
    'braco_esquerdo','panturrilha_direita','face_temporal_direita','pescoco_posterior'),
  ROUND(3.0 + (f.ficha_id*7 % 200)/10.0, 1),
  JSON_OBJECT(
    'assimetria',   IF(f.ficha_id%3 != 0, TRUE, FALSE),
    'borda',        ELT(1+(f.ficha_id%4),'regular','irregular','irregular_perolada','indefinida'),
    'cor',          ELT(1+(f.ficha_id%5),'marrom_uniforme','multicolorida','negra','rosea','marrom_escura'),
    'diametro_mm',  ROUND(3.0+(f.ficha_id*7%200)/10.0, 1)
  ),
  CONCAT('s3://ida-imagens/lesoes/b1_', f.ficha_id, '.jpg'),
  IF(f.ficha_id%2=0, 1200, 600),
  ELT(1+(f.ficha_id%3),
    'Dermatoscópio Heine Delta 30',
    'Dermatoscópio FotoFinder Medicam 1000',
    'Dermatoscópio Dermoscopy DL100'),
  DATE_ADD(f.data_primeiro_atendimento, INTERVAL (f.ficha_id%15) DAY),
  ELT(1+(f.ficha_id%3),'CRM123456','CRM789012','CRM345678')
FROM triagem.ficha_dermato f
WHERE f.cpf_titular BETWEEN '00000001001' AND '00000002000';

-- --------------------------------------------------------
-- Segunda lesão para metade dos pacientes (~500 lesões)
-- --------------------------------------------------------
INSERT INTO lesoes_fotografadas
  (ficha_id, localizacao_anatomica, tamanho_mm, formato_abcde,
   caminho_imagem_s3, resolucao_dpi, equipamento_usado, data_foto, revisado_por)
SELECT
  f.ficha_id,
  ELT(1+((f.ficha_id+5)%10),
    'escapula_direita','escapula_esquerda','dorso_superior_esquerdo',
    'dorso_inferior_medio','tronco_anterior_superior','braco_direito',
    'braco_esquerdo','panturrilha_direita','face_temporal_direita','pescoco_posterior'),
  ROUND(2.0 + (f.ficha_id*11 % 150)/10.0, 1),
  JSON_OBJECT(
    'assimetria', IF(f.ficha_id%7=0, TRUE, FALSE),
    'borda',      'regular',
    'cor',        'marrom_uniforme',
    'diametro_mm', ROUND(2.0+(f.ficha_id*11%150)/10.0, 1)
  ),
  CONCAT('s3://ida-imagens/lesoes/b2_', f.ficha_id, '.jpg'),
  600,
  'Dermatoscópio Heine Delta 30',
  DATE_ADD(f.data_primeiro_atendimento, INTERVAL ((f.ficha_id%20)+5) DAY),
  ELT(1+(f.ficha_id%3),'CRM123456','CRM789012','CRM345678')
FROM triagem.ficha_dermato f
WHERE f.cpf_titular BETWEEN '00000001001' AND '00000002000'
  AND f.ficha_id % 2 = 0;

-- --------------------------------------------------------
-- Agendamentos (~1500 registros)
-- --------------------------------------------------------
INSERT INTO agendamentos_ida
  (ficha_id, medico_crm, modalidade, data_hora_marcada, sala_numero, status_agenda)
SELECT
  f.ficha_id,
  ELT(1+(f.ficha_id%3),'CRM123456','CRM789012','CRM345678'),
  IF(f.ficha_id%6=0,'teleconsulta','presencial'),
  DATE_ADD(f.data_primeiro_atendimento, INTERVAL ((f.ficha_id%5)*21) DAY),
  IF(f.ficha_id%6=0, NULL, 1+(f.ficha_id%8)),
  ELT(1+(f.ficha_id%5),'realizado','realizado','realizado','confirmado','cancelado')
FROM triagem.ficha_dermato f
WHERE f.cpf_titular BETWEEN '00000001001' AND '00000002000';

-- Segundo agendamento para metade
INSERT INTO agendamentos_ida
  (ficha_id, medico_crm, modalidade, data_hora_marcada, sala_numero, status_agenda)
SELECT
  f.ficha_id,
  ELT(1+((f.ficha_id+1)%3),'CRM123456','CRM789012','CRM345678'),
  IF(f.ficha_id%4=0,'teleconsulta','presencial'),
  DATE_ADD(f.data_primeiro_atendimento, INTERVAL ((f.ficha_id%5+1)*21+7) DAY),
  IF(f.ficha_id%5=0, NULL, 1+((f.ficha_id+3)%8)),
  ELT(1+(f.ficha_id%4),'realizado','realizado','confirmado','pendente')
FROM triagem.ficha_dermato f
WHERE f.cpf_titular BETWEEN '00000001001' AND '00000002000'
  AND f.ficha_id % 2 = 0;

-- ============================================================
-- SCHEMA: anatomopatologia
-- ============================================================
USE anatomopatologia;

-- --------------------------------------------------------
-- Biópsias para ~667 pacientes (ficha_id % 3 != 2)
-- Uma biópsia por paciente usando a primeira lesão
-- --------------------------------------------------------
INSERT INTO biopsia_solicitacao
  (lesao_id, medico_solicitante_crm, tecnica_biopsia, urgencia,
   data_solicitacao, observacoes_clinicas)
SELECT
  MIN(l.lesao_id),
  ELT(1+(f.ficha_id%3),'CRM123456','CRM789012','CRM345678'),
  ELT(1+(f.ficha_id%5),'punch','excisional','incisional','shave','curetagem'),
  IF(f.ficha_id%10 < 2, 'urgente', 'eletiva'),
  DATE_ADD(f.data_primeiro_atendimento, INTERVAL ((f.ficha_id%10)+5) DAY),
  NULL
FROM triagem.lesoes_fotografadas l
JOIN triagem.ficha_dermato f ON f.ficha_id = l.ficha_id
WHERE f.cpf_titular BETWEEN '00000001001' AND '00000002000'
  AND f.ficha_id % 3 != 2
GROUP BY f.ficha_id, f.data_primeiro_atendimento;

-- --------------------------------------------------------
-- Laudos histopatológicos (1 por biópsia)
-- Distribuição diagnóstica:
--   ficha%6=0 → Melanoma extensão superficial (C435)
--   ficha%6=1 → Melanoma nodular (C439)
--   ficha%6=2 → Carcinoma basocelular nodular (C440)  [excluído por ficha%3=2]
--   ficha%6=3 → Carcinoma basocelular superficial (C441)
--   ficha%6=4 → Nevo displásico (D036)
--   ficha%6=5 → Nevo benigno (D229)
-- --------------------------------------------------------
INSERT INTO laudo_histopatologico
  (solicitacao_id, patologista_crm, classificacao_who,
   espessura_breslow_mm, nivel_clark, margem_livre,
   indice_mitose, estadiamento_tnm, codigo_cid10,
   data_laudo, numero_protocolo)
SELECT
  bs.solicitacao_id,
  ELT(1+(bs.solicitacao_id%2),'CRM_PAT001','CRM_PAT002'),
  ELT(1+(f.ficha_id%6),
    'Melanoma maligno, extensão superficial',
    'Melanoma maligno nodular',
    'Carcinoma basocelular nodular',
    'Carcinoma basocelular superficial',
    'Nevo melanocítico displásico de alto grau',
    'Nevo melanocítico composto — benigno'),
  CASE WHEN f.ficha_id%6 IN (0,1)
    THEN ROUND(0.5 + (f.ficha_id%40)/10.0, 2) ELSE NULL END,
  1 + (f.ficha_id%5),
  (f.ficha_id%4 != 0),
  CASE WHEN f.ficha_id%6 IN (0,1)
    THEN ROUND((f.ficha_id%30)/5.0, 1) ELSE 0.0 END,
  CASE
    WHEN f.ficha_id%6=0 THEN ELT(1+(f.ficha_id%4),'T1aN0M0','T2aN0M0','T2bN0M0','T3aN1M0')
    WHEN f.ficha_id%6=1 THEN ELT(1+(f.ficha_id%4),'T2bN0M0','T3aN0M0','T3bN1M0','T4bN1M0')
    ELSE NULL
  END,
  ELT(1+(f.ficha_id%6),'C435','C439','C440','C441','D036','D229'),
  DATE_ADD(f.data_primeiro_atendimento, INTERVAL ((f.ficha_id%10)+15) DAY),
  CONCAT('PROT-IDA-BULK-', LPAD(bs.solicitacao_id, 6, '0'))
FROM anatomopatologia.biopsia_solicitacao bs
JOIN triagem.lesoes_fotografadas l  ON l.lesao_id  = bs.lesao_id
JOIN triagem.ficha_dermato       f  ON f.ficha_id  = l.ficha_id
WHERE f.cpf_titular BETWEEN '00000001001' AND '00000002000';

-- --------------------------------------------------------
-- Imunoistoquímica apenas para melanomas (ficha%6 IN (0,1))
-- 3 marcadores por laudo → ~400 laudos × 3 = ~1200 marcadores
-- --------------------------------------------------------
INSERT INTO imunohistoquimica (laudo_id, anticorpo, resultado, intensidade, percentual_celulas)
SELECT lh.laudo_id, 'S-100', 'positivo',
  ELT(1+(lh.laudo_id%3),'fraca','moderada','forte'),
  60.0 + (lh.laudo_id%40)
FROM anatomopatologia.laudo_histopatologico lh
JOIN anatomopatologia.biopsia_solicitacao   bs ON bs.solicitacao_id = lh.solicitacao_id
JOIN triagem.lesoes_fotografadas            l  ON l.lesao_id        = bs.lesao_id
JOIN triagem.ficha_dermato                 f  ON f.ficha_id         = l.ficha_id
WHERE f.cpf_titular BETWEEN '00000001001' AND '00000002000'
  AND f.ficha_id%6 IN (0,1);

INSERT INTO imunohistoquimica (laudo_id, anticorpo, resultado, intensidade, percentual_celulas)
SELECT lh.laudo_id, 'HMB-45',
  IF(lh.laudo_id%7=0,'negativo','positivo'),
  ELT(1+(lh.laudo_id%3),'fraca','moderada','forte'),
  40.0 + (lh.laudo_id%55)
FROM anatomopatologia.laudo_histopatologico lh
JOIN anatomopatologia.biopsia_solicitacao   bs ON bs.solicitacao_id = lh.solicitacao_id
JOIN triagem.lesoes_fotografadas            l  ON l.lesao_id        = bs.lesao_id
JOIN triagem.ficha_dermato                 f  ON f.ficha_id         = l.ficha_id
WHERE f.cpf_titular BETWEEN '00000001001' AND '00000002000'
  AND f.ficha_id%6 IN (0,1);

INSERT INTO imunohistoquimica (laudo_id, anticorpo, resultado, intensidade, percentual_celulas)
SELECT lh.laudo_id, 'BRAF V600E',
  IF(lh.laudo_id%3=0,'positivo','negativo'),
  IF(lh.laudo_id%3=0, ELT(1+(lh.laudo_id%2),'moderada','forte'), NULL),
  IF(lh.laudo_id%3=0, 80.0+(lh.laudo_id%15), NULL)
FROM anatomopatologia.laudo_histopatologico lh
JOIN anatomopatologia.biopsia_solicitacao   bs ON bs.solicitacao_id = lh.solicitacao_id
JOIN triagem.lesoes_fotografadas            l  ON l.lesao_id        = bs.lesao_id
JOIN triagem.ficha_dermato                 f  ON f.ficha_id         = l.ficha_id
WHERE f.cpf_titular BETWEEN '00000001001' AND '00000002000'
  AND f.ficha_id%6 IN (0,1);

INSERT INTO imunohistoquimica (laudo_id, anticorpo, resultado, intensidade, percentual_celulas)
SELECT lh.laudo_id, 'Ki-67', 'positivo',
  ELT(1+(lh.laudo_id%3),'fraca','moderada','forte'),
  5.0 + (lh.laudo_id%60)
FROM anatomopatologia.laudo_histopatologico lh
JOIN anatomopatologia.biopsia_solicitacao   bs ON bs.solicitacao_id = lh.solicitacao_id
JOIN triagem.lesoes_fotografadas            l  ON l.lesao_id        = bs.lesao_id
JOIN triagem.ficha_dermato                 f  ON f.ficha_id         = l.ficha_id
WHERE f.cpf_titular BETWEEN '00000001001' AND '00000002000'
  AND f.ficha_id%6 IN (0,1);

-- ============================================================
-- SCHEMA: oncologia_pele
-- ============================================================
USE oncologia_pele;

-- --------------------------------------------------------
-- Protocolos para melanomas
-- --------------------------------------------------------
INSERT INTO protocolo_tratamento
  (laudo_id, oncologista_crm, modalidade_terapia, medicamento_principal,
   dose_mg, ciclos_previstos, intervalo_dias,
   data_inicio, data_fim_prevista, status_protocolo)
SELECT
  lh.laudo_id,
  ELT(1+(lh.laudo_id%2),'CRM_ONC001','CRM_ONC002'),
  CASE
    WHEN (f.ficha_id/6)%3=0 THEN 'imunoterapia'
    WHEN (f.ficha_id/6)%3=1 THEN 'terapia_alvo'
    ELSE                          'combinado'
  END,
  CASE
    WHEN (f.ficha_id/6)%3=0 THEN 'Pembrolizumab'
    WHEN (f.ficha_id/6)%3=1 THEN 'Vemurafenib'
    ELSE                          'Nivolumab'
  END,
  CASE
    WHEN (f.ficha_id/6)%3=0 THEN 200.000
    WHEN (f.ficha_id/6)%3=1 THEN 960.000
    ELSE                          240.000
  END,
  ELT(1+(f.ficha_id%3), 6, 8, 18),
  21,
  DATE_ADD(f.data_primeiro_atendimento, INTERVAL ((f.ficha_id%10)+20) DAY),
  DATE_ADD(f.data_primeiro_atendimento, INTERVAL ((f.ficha_id%10)+20+(21*ELT(1+(f.ficha_id%3),6,8,18))) DAY),
  ELT(1+(f.ficha_id%3),'ativo','ativo','concluido')
FROM anatomopatologia.laudo_histopatologico lh
JOIN anatomopatologia.biopsia_solicitacao   bs ON bs.solicitacao_id = lh.solicitacao_id
JOIN triagem.lesoes_fotografadas            l  ON l.lesao_id        = bs.lesao_id
JOIN triagem.ficha_dermato                 f  ON f.ficha_id         = l.ficha_id
WHERE f.cpf_titular BETWEEN '00000001001' AND '00000002000'
  AND lh.classificacao_who LIKE 'Melanoma%';

-- --------------------------------------------------------
-- Evoluções clínicas: 3 por protocolo (~3 × qtd protocolos)
-- --------------------------------------------------------
-- Ciclo 1
INSERT INTO evolucao_clinica
  (protocolo_id, data_consulta, ciclo_atual, resposta_ecog,
   dimensao_lesao_mm, toxicidade_grau, ajuste_dose, anotacao_medica)
SELECT
  p.protocolo_id,
  DATE_ADD(p.data_inicio, INTERVAL 0 DAY),
  1,
  1 + (p.protocolo_id%3),
  ROUND(10.0 + (p.protocolo_id%100)/10.0, 1),
  p.protocolo_id%3,
  FALSE,
  NULL
FROM oncologia_pele.protocolo_tratamento p
JOIN anatomopatologia.laudo_histopatologico lh ON lh.laudo_id = p.laudo_id
JOIN anatomopatologia.biopsia_solicitacao   bs ON bs.solicitacao_id = lh.solicitacao_id
JOIN triagem.lesoes_fotografadas            l  ON l.lesao_id  = bs.lesao_id
JOIN triagem.ficha_dermato                 f  ON f.ficha_id   = l.ficha_id
WHERE f.cpf_titular BETWEEN '00000001001' AND '00000002000';

-- Ciclo 2
INSERT INTO evolucao_clinica
  (protocolo_id, data_consulta, ciclo_atual, resposta_ecog,
   dimensao_lesao_mm, toxicidade_grau, ajuste_dose, anotacao_medica)
SELECT
  p.protocolo_id,
  DATE_ADD(p.data_inicio, INTERVAL 21 DAY),
  2,
  p.protocolo_id%3,
  ROUND(7.0 + (p.protocolo_id%80)/10.0, 1),
  (p.protocolo_id+1)%3,
  FALSE,
  NULL
FROM oncologia_pele.protocolo_tratamento p
JOIN anatomopatologia.laudo_histopatologico lh ON lh.laudo_id = p.laudo_id
JOIN anatomopatologia.biopsia_solicitacao   bs ON bs.solicitacao_id = lh.solicitacao_id
JOIN triagem.lesoes_fotografadas            l  ON l.lesao_id  = bs.lesao_id
JOIN triagem.ficha_dermato                 f  ON f.ficha_id   = l.ficha_id
WHERE f.cpf_titular BETWEEN '00000001001' AND '00000002000';

-- Ciclo 3
INSERT INTO evolucao_clinica
  (protocolo_id, data_consulta, ciclo_atual, resposta_ecog,
   dimensao_lesao_mm, toxicidade_grau, ajuste_dose, anotacao_medica)
SELECT
  p.protocolo_id,
  DATE_ADD(p.data_inicio, INTERVAL 42 DAY),
  3,
  (p.protocolo_id+2)%3,
  ROUND(3.0 + (p.protocolo_id%60)/10.0, 1),
  (p.protocolo_id+2)%3,
  (p.protocolo_id%5=0),
  NULL
FROM oncologia_pele.protocolo_tratamento p
JOIN anatomopatologia.laudo_histopatologico lh ON lh.laudo_id = p.laudo_id
JOIN anatomopatologia.biopsia_solicitacao   bs ON bs.solicitacao_id = lh.solicitacao_id
JOIN triagem.lesoes_fotografadas            l  ON l.lesao_id  = bs.lesao_id
JOIN triagem.ficha_dermato                 f  ON f.ficha_id   = l.ficha_id
WHERE f.cpf_titular BETWEEN '00000001001' AND '00000002000';

SET FOREIGN_KEY_CHECKS = 1;
SET UNIQUE_CHECKS     = 1;

SELECT 'IDA MySQL bulk seed concluído!' AS status;
