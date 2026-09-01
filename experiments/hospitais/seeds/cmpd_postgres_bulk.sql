-- ============================================================
-- CMPD — PostgreSQL: bulk seed de 1500 novos beneficiários
-- CPF range: 00000001001..00000002500
-- CPFs 00000001001..00000001700 aparecem também no IDA (federados)
-- CPFs 00000001701..00000002000 aparecem no IDA mas não aqui (IDA exclusivo)
-- CPFs 00000002001..00000002500 são exclusivos do CMPD
-- ============================================================

SET statement_timeout = 0;

-- Arrays reutilizados como CTEs
-- ============================================================
-- 1. cadastro_beneficiario.perfil_saude — 1500 linhas
-- ============================================================
INSERT INTO cadastro_beneficiario.perfil_saude
  (numero_cpf, prenome, sobrenome, nascimento, genero_identidade,
   telefone_celular, email_contato, cep_residencia, municipio, uf_sigla,
   plano_saude_ans, renda_faixa, fumante_status, pratica_atividade_fisica, imc_atual)
SELECT
  LPAD(gs::TEXT, 11, '0'),
  -- prenome baseado em gs%25
  (ARRAY['João','Maria','Carlos','Ana','Pedro','Fernanda','Roberto','Juliana',
         'Ricardo','Patrícia','Eduardo','Camila','Marcos','Beatriz','Lucas',
         'Amanda','Bruno','Larissa','Diego','Vanessa','Rafael','Letícia',
         'André','Gabriela','Paulo'])[1 + (gs%25)],
  -- sobrenome baseado em (gs*7)%25
  (ARRAY['Silva','Santos','Oliveira','Pereira','Costa','Rodrigues','Alves',
         'Nascimento','Lima','Araújo','Ferreira','Carvalho','Melo','Barbosa',
         'Ribeiro','Castro','Monteiro','Gomes','Souza','Freitas','Moreira',
         'Xavier','Cardoso','Mendes','Correia'])[1 + ((gs*7)%25)],
  '1945-01-01'::DATE + ((gs*13) % 21900) * INTERVAL '1 day',
  CASE WHEN gs%2=0 THEN 'feminino' ELSE 'masculino' END,
  '(' || (11 + gs%89)::TEXT || ') 9' || LPAD((gs*7%99999)::TEXT,8,'0'),
  LOWER(
    (ARRAY['joao','maria','carlos','ana','pedro','fernanda','roberto','juliana',
           'ricardo','patricia','eduardo','camila','marcos','beatriz','lucas',
           'amanda','bruno','larissa','diego','vanessa','rafael','leticia',
           'andre','gabriela','paulo'])[1+(gs%25)]
  ) || '.' ||
  LOWER(
    (ARRAY['silva','santos','oliveira','pereira','costa','rodrigues','alves',
           'nascimento','lima','araujo','ferreira','carvalho','melo','barbosa',
           'ribeiro','castro','monteiro','gomes','souza','freitas','moreira',
           'xavier','cardoso','mendes','correia'])[1+((gs*7)%25)]
  ) || gs::TEXT || '@email.com',
  LPAD((gs*17%99999)::TEXT, 8, '0'),
  (ARRAY['São Paulo','Rio de Janeiro','Belo Horizonte','Brasília','Salvador',
         'Fortaleza','Curitiba','Manaus','Recife','Porto Alegre',
         'Belém','Goiânia','Guarulhos','Campinas','São Luís',
         'Maceió','Natal','Teresina','Campo Grande','João Pessoa'])[1+(gs%20)],
  (ARRAY['SP','RJ','MG','DF','BA','CE','PR','AM','PE','RS',
         'PA','GO','SP','SP','MA','AL','RN','PI','MS','PB'])[1+(gs%20)],
  CASE WHEN gs%8=0 THEN NULL
       ELSE 'ANS' || LPAD((gs%9999)::TEXT, 5, '0') END,
  (ARRAY['A','B','C','C','D','D','E'])[1+(gs%7)],
  (ARRAY['nunca','nunca','nunca','ex','ex','atual'])[1+(gs%6)],
  (gs%3 != 0),
  18.5 + ROUND(((gs*17)%300)::NUMERIC/10.0, 1)
FROM generate_series(1001, 2500) AS gs;

-- ============================================================
-- 2. cadastro_beneficiario.antecedentes_clinicos
--    ~1200 linhas (80% dos pacientes têm ao menos 1 antecedente)
-- ============================================================
INSERT INTO cadastro_beneficiario.antecedentes_clinicos
  (beneficiario_id, grupo_cid11, descricao_condicao, ano_diagnostico,
   em_tratamento_ativo, medicacao_continua, gravidade)
SELECT
  ps.beneficiario_id,
  (ARRAY['BA00','5A11','5B81','5A00','BA80','MA90','8A00','CB00','CA40','FA24']
  )[1 + (ps.beneficiario_id%10)],
  (ARRAY[
    'Hipertensão arterial sistêmica',
    'Diabetes mellitus tipo 2',
    'Sobrepeso (IMC 25-29.9)',
    'Hipotireoidismo primário',
    'Insuficiência cardíaca crônica',
    'Asma brônquica moderada',
    'Depressão moderada recorrente',
    'Doença pulmonar obstrutiva crônica',
    'Dislipidemia mista',
    'Ansiedade generalizada'
  ])[1 + (ps.beneficiario_id%10)],
  2005 + (ps.beneficiario_id%18),
  (ps.beneficiario_id%3 != 0),
  CASE (ps.beneficiario_id%10)
    WHEN 0 THEN 'Losartana 50mg/dia'
    WHEN 1 THEN 'Metformina 1000mg 2x/dia'
    WHEN 2 THEN NULL
    WHEN 3 THEN 'Levotiroxina 75mcg/dia'
    WHEN 4 THEN 'Carvedilol 25mg 2x/dia, Sacubitril/Valsartana 200mg 2x/dia'
    WHEN 5 THEN 'Fluticasona/Salmeterol 250/25mcg 2x/dia'
    WHEN 6 THEN 'Sertralina 50mg/dia'
    WHEN 7 THEN 'Budesonida/Formoterol 160/4.5mcg 2x/dia'
    WHEN 8 THEN 'Rosuvastatina 20mg/dia'
    ELSE        'Clonazepam 0.5mg/noite'
  END,
  (ARRAY['leve','leve','moderada','moderada','grave'])[1+(ps.beneficiario_id%5)]
FROM cadastro_beneficiario.perfil_saude ps
WHERE ps.numero_cpf BETWEEN '00000001001' AND '00000002500'
  AND ps.beneficiario_id % 5 != 0;   -- 80%

-- Segundo antecedente para 30% dos pacientes
INSERT INTO cadastro_beneficiario.antecedentes_clinicos
  (beneficiario_id, grupo_cid11, descricao_condicao, ano_diagnostico,
   em_tratamento_ativo, medicacao_continua, gravidade)
SELECT
  ps.beneficiario_id,
  (ARRAY['CA40','5A11','BA00','FA24','8A00','CB00','5B81','MA90']
  )[1 + ((ps.beneficiario_id*3)%8)],
  (ARRAY[
    'Dislipidemia mista',
    'Diabetes mellitus tipo 2',
    'Hipertensão arterial sistêmica',
    'Ansiedade generalizada',
    'Depressão moderada recorrente',
    'Doença pulmonar obstrutiva crônica',
    'Obesidade grau I',
    'Asma brônquica leve'
  ])[1 + ((ps.beneficiario_id*3)%8)],
  2010 + (ps.beneficiario_id%13),
  (ps.beneficiario_id%4 != 0),
  NULL,
  'leve'
FROM cadastro_beneficiario.perfil_saude ps
WHERE ps.numero_cpf BETWEEN '00000001001' AND '00000002500'
  AND ps.beneficiario_id % 3 = 0;    -- 33%

-- ============================================================
-- 3. cadastro_beneficiario.indicadores_vitais
--    2 aferimentos por paciente (~3000 linhas)
-- ============================================================
-- Aferimento recente (últimos 2 anos)
INSERT INTO cadastro_beneficiario.indicadores_vitais
  (beneficiario_id, data_aferimento, pressao_sistolica, pressao_diastolica,
   glicemia_jejum_mg, colesterol_total, hdl, ldl, triglicerideos,
   saturacao_o2, peso_kg, altura_cm)
SELECT
  ps.beneficiario_id,
  NOW() - ((ps.beneficiario_id % 730) * INTERVAL '1 day'),
  100 + (ps.beneficiario_id % 80),                                  -- 100..179 mmHg
  60  + (ps.beneficiario_id % 50),                                  -- 60..109 mmHg
  75.0 + ROUND(((ps.beneficiario_id*7) % 1600)::NUMERIC/10.0, 1),  -- 75..234.9 mg/dL
  140.0 + ROUND(((ps.beneficiario_id*11) % 1800)::NUMERIC/10.0,1), -- 140..319.9 mg/dL
  30.0 + ROUND(((ps.beneficiario_id*13) % 500)::NUMERIC/10.0, 1),  -- 30..79.9 mg/dL
  80.0 + ROUND(((ps.beneficiario_id*17) % 1400)::NUMERIC/10.0, 1), -- 80..219.9 mg/dL
  60.0 + ROUND(((ps.beneficiario_id*19) % 3400)::NUMERIC/10.0, 1), -- 60..399.9 mg/dL
  92.0 + ROUND(((ps.beneficiario_id) % 80)::NUMERIC/10.0, 1),      -- 92..99.9 %
  45.0 + ROUND(((ps.beneficiario_id*23) % 600)::NUMERIC/10.0, 1),  -- 45..104.9 kg
  145.0 + (ps.beneficiario_id % 45)                                 -- 145..189 cm
FROM cadastro_beneficiario.perfil_saude ps
WHERE ps.numero_cpf BETWEEN '00000001001' AND '00000002500';

-- Aferimento anterior (1 ano atrás)
INSERT INTO cadastro_beneficiario.indicadores_vitais
  (beneficiario_id, data_aferimento, pressao_sistolica, pressao_diastolica,
   glicemia_jejum_mg, colesterol_total, hdl, ldl, triglicerideos,
   saturacao_o2, peso_kg, altura_cm)
SELECT
  ps.beneficiario_id,
  NOW() - ((ps.beneficiario_id % 730) * INTERVAL '1 day') - INTERVAL '1 year',
  100 + ((ps.beneficiario_id+5) % 80),
  60  + ((ps.beneficiario_id+5) % 50),
  75.0 + ROUND((((ps.beneficiario_id+5)*7) % 1600)::NUMERIC/10.0, 1),
  140.0 + ROUND((((ps.beneficiario_id+5)*11) % 1800)::NUMERIC/10.0,1),
  30.0 + ROUND((((ps.beneficiario_id+5)*13) % 500)::NUMERIC/10.0, 1),
  80.0 + ROUND((((ps.beneficiario_id+5)*17) % 1400)::NUMERIC/10.0, 1),
  60.0 + ROUND((((ps.beneficiario_id+5)*19) % 3400)::NUMERIC/10.0, 1),
  92.0 + ROUND(((ps.beneficiario_id+5) % 80)::NUMERIC/10.0, 1),
  45.0 + ROUND((((ps.beneficiario_id+5)*23) % 600)::NUMERIC/10.0, 1),
  145.0 + ((ps.beneficiario_id+5) % 45)
FROM cadastro_beneficiario.perfil_saude ps
WHERE ps.numero_cpf BETWEEN '00000001001' AND '00000002500';

-- ============================================================
-- 4. rastreamento.inscricao_programa
--    1-2 inscrições por paciente (programa_id 1..5 já existem)
-- ============================================================
-- Primeira inscrição
INSERT INTO rastreamento.inscricao_programa
  (beneficiario_id, programa_id, data_ingresso, proxima_realizacao,
   risco_calculado, motivo_ingresso)
SELECT
  ps.beneficiario_id,
  1 + (ps.beneficiario_id % 5),
  CURRENT_DATE - ((ps.beneficiario_id % 365) * INTERVAL '1 day'),
  CURRENT_DATE + ((12 - (ps.beneficiario_id % 12)) * INTERVAL '1 month'),
  (ARRAY['baixo','intermediario','intermediario','alto','muito_alto'])[1+(ps.beneficiario_id%5)],
  (ARRAY[
    'Faixa etária de risco — rastreamento de rotina',
    'Histórico familiar positivo',
    'Fator de risco cardiovascular presente',
    'Resultado alterado em exame prévio',
    'Encaminhamento por outro especialista'
  ])[1+(ps.beneficiario_id%5)]
FROM cadastro_beneficiario.perfil_saude ps
WHERE ps.numero_cpf BETWEEN '00000001001' AND '00000002500'
ON CONFLICT (beneficiario_id, programa_id) DO NOTHING;

-- Segunda inscrição para 50% dos pacientes
INSERT INTO rastreamento.inscricao_programa
  (beneficiario_id, programa_id, data_ingresso, proxima_realizacao,
   risco_calculado, motivo_ingresso)
SELECT
  ps.beneficiario_id,
  1 + ((ps.beneficiario_id * 3 + 2) % 5),
  CURRENT_DATE - (((ps.beneficiario_id+7) % 365) * INTERVAL '1 day'),
  CURRENT_DATE + ((6 + (ps.beneficiario_id % 6)) * INTERVAL '1 month'),
  (ARRAY['baixo','intermediario','alto','alto','muito_alto'])[1+((ps.beneficiario_id*2)%5)],
  'Múltiplos fatores de risco identificados na consulta preventiva'
FROM cadastro_beneficiario.perfil_saude ps
WHERE ps.numero_cpf BETWEEN '00000001001' AND '00000002500'
  AND ps.beneficiario_id % 2 = 0
ON CONFLICT (beneficiario_id, programa_id) DO NOTHING;

-- ============================================================
-- 5. laboratorio.requisicao_exame — 1 por paciente (~1500 linhas)
-- ============================================================
INSERT INTO laboratorio.requisicao_exame
  (beneficiario_id, medico_requisitante, painel_exames, prioridade,
   inscricao_id, data_emissao, instrucoes_preparo)
SELECT
  ps.beneficiario_id,
  (ARRAY['CRM_PREV001','CRM_PREV002','CRM_PREV003','CRM_PREV004'])[1+(ps.beneficiario_id%4)],
  CASE ps.beneficiario_id % 5
    WHEN 0 THEN '["Hemograma completo","Lipidograma","Glicemia jejum","Creatinina","TSH"]'
    WHEN 1 THEN '["HbA1c","Glicemia jejum","Microalbuminúria","Creatinina","Lipidograma"]'
    WHEN 2 THEN '["PSA total","PSA livre","Creatinina","Hemograma"]'
    WHEN 3 THEN '["Mamografia bilateral","TSH","T4 livre","Hemograma"]'
    ELSE        '["BNP","Troponina I","Creatinina","Lipidograma","Hemograma","ECG"]'
  END::JSONB,
  CASE WHEN ps.beneficiario_id % 15 = 0 THEN 'urgente' ELSE 'rotina' END,
  ip.inscricao_id,
  CURRENT_DATE - ((ps.beneficiario_id % 365) * INTERVAL '1 day'),
  CASE ps.beneficiario_id % 4
    WHEN 0 THEN 'Jejum de 12h. Suspender estatinas 48h antes se orientado.'
    WHEN 1 THEN 'Jejum de 8h. Coletar urina de 24h com frasco fornecido.'
    WHEN 2 THEN 'Abstinência sexual 48h antes. Sem atividade física intensa 24h antes.'
    ELSE        'Não usar desodorante, talco ou creme axilar no dia do exame.'
  END
FROM cadastro_beneficiario.perfil_saude ps
LEFT JOIN LATERAL (
  SELECT inscricao_id
  FROM rastreamento.inscricao_programa ip2
  WHERE ip2.beneficiario_id = ps.beneficiario_id
  ORDER BY ip2.inscricao_id
  LIMIT 1
) ip ON TRUE
WHERE ps.numero_cpf BETWEEN '00000001001' AND '00000002500';

-- ============================================================
-- 6. laboratorio.resultado_exame — 3 analitos por requisição
--    ~4500 linhas
-- ============================================================
-- Analito 1 (baseado no painel)
INSERT INTO laboratorio.resultado_exame
  (requisicao_id, analito, valor_numerico, unidade_medida,
   valor_referencia_min, valor_referencia_max, interpretacao,
   metodo_analitico, data_coleta, data_resultado, laboratorio_executante)
SELECT
  re.requisicao_id,
  CASE re.requisicao_id % 5
    WHEN 0 THEN 'Hemoglobina'
    WHEN 1 THEN 'HbA1c'
    WHEN 2 THEN 'PSA total'
    WHEN 3 THEN 'Mamografia — Densidade mamária'
    ELSE        'BNP'
  END,
  CASE re.requisicao_id % 5
    WHEN 0 THEN ROUND((100.0 + (re.requisicao_id%80))/10.0, 1)   -- 10.0..17.9 g/dL
    WHEN 1 THEN ROUND((40.0 + (re.requisicao_id%80))/10.0, 1)    -- 4.0..11.9 %
    WHEN 2 THEN ROUND((re.requisicao_id%100)/10.0, 2)             -- 0.0..9.9 ng/mL
    WHEN 3 THEN NULL
    ELSE        50.0 + (re.requisicao_id%500)                      -- 50..549 pg/mL
  END,
  CASE re.requisicao_id % 5
    WHEN 0 THEN 'g/dL'
    WHEN 1 THEN '%'
    WHEN 2 THEN 'ng/mL'
    WHEN 3 THEN NULL
    ELSE        'pg/mL'
  END,
  CASE re.requisicao_id % 5
    WHEN 0 THEN 12.0 WHEN 1 THEN NULL WHEN 2 THEN NULL WHEN 3 THEN NULL ELSE NULL
  END,
  CASE re.requisicao_id % 5
    WHEN 0 THEN 17.5 WHEN 1 THEN 5.7  WHEN 2 THEN 4.0  WHEN 3 THEN NULL ELSE 100.0
  END,
  CASE re.requisicao_id % 5
    WHEN 0 THEN CASE WHEN (100+(re.requisicao_id%80))/10.0 >= 12.0 THEN 'normal' ELSE 'alterado_moderado' END
    WHEN 1 THEN CASE WHEN (40+(re.requisicao_id%80))/10.0 <= 5.7 THEN 'normal'
                     WHEN (40+(re.requisicao_id%80))/10.0 <= 6.4 THEN 'alterado_leve'
                     WHEN (40+(re.requisicao_id%80))/10.0 <= 8.0 THEN 'alterado_moderado'
                     ELSE 'critico' END
    WHEN 2 THEN CASE WHEN (re.requisicao_id%100)/10.0 <= 4.0 THEN 'normal' ELSE 'alterado_leve' END
    WHEN 3 THEN 'normal'
    ELSE        CASE WHEN 50+(re.requisicao_id%500) <= 100 THEN 'normal'
                     WHEN 50+(re.requisicao_id%500) <= 300 THEN 'alterado_leve'
                     ELSE 'critico' END
  END,
  (ARRAY['Citometria de fluxo','Cromatografia HPLC','Quimioluminescência ECLIA',
         'Laudado pelo radiologista','Quimioluminescência ARCHITECT'])[1+(re.requisicao_id%5)],
  re.data_emissao + INTERVAL '1 day',
  re.data_emissao + INTERVAL '2 days',
  (ARRAY['Lab CMPD Central','Lab CMPD Urgência','Lab CMPD Filial Norte'])[1+(re.requisicao_id%3)]
FROM laboratorio.requisicao_exame re
JOIN cadastro_beneficiario.perfil_saude ps ON ps.beneficiario_id = re.beneficiario_id
WHERE ps.numero_cpf BETWEEN '00000001001' AND '00000002500';

-- Analito 2 (colesterol total)
INSERT INTO laboratorio.resultado_exame
  (requisicao_id, analito, valor_numerico, unidade_medida,
   valor_referencia_min, valor_referencia_max, interpretacao,
   metodo_analitico, data_coleta, data_resultado, laboratorio_executante)
SELECT
  re.requisicao_id,
  'Colesterol total',
  140.0 + ROUND(((re.requisicao_id*11) % 2000)::NUMERIC/10.0, 1),
  'mg/dL',
  NULL, 190.0,
  CASE WHEN 140.0+((re.requisicao_id*11)%2000)/10.0 <= 190.0 THEN 'normal'
       WHEN 140.0+((re.requisicao_id*11)%2000)/10.0 <= 240.0 THEN 'alterado_leve'
       ELSE 'alterado_moderado'
  END,
  'Enzimático colorimétrico',
  re.data_emissao + INTERVAL '1 day',
  re.data_emissao + INTERVAL '2 days',
  (ARRAY['Lab CMPD Central','Lab CMPD Urgência','Lab CMPD Filial Norte'])[1+(re.requisicao_id%3)]
FROM laboratorio.requisicao_exame re
JOIN cadastro_beneficiario.perfil_saude ps ON ps.beneficiario_id = re.beneficiario_id
WHERE ps.numero_cpf BETWEEN '00000001001' AND '00000002500';

-- Analito 3 (creatinina)
INSERT INTO laboratorio.resultado_exame
  (requisicao_id, analito, valor_numerico, unidade_medida,
   valor_referencia_min, valor_referencia_max, interpretacao,
   metodo_analitico, data_coleta, data_resultado, laboratorio_executante)
SELECT
  re.requisicao_id,
  'Creatinina',
  ROUND((5.0 + (re.requisicao_id % 20))::NUMERIC/10.0, 2),   -- 0.5..2.4 mg/dL
  'mg/dL',
  0.6, 1.2,
  CASE WHEN (5+(re.requisicao_id%20))::NUMERIC/10.0 <= 1.2 THEN 'normal'
       WHEN (5+(re.requisicao_id%20))::NUMERIC/10.0 <= 1.8 THEN 'alterado_leve'
       ELSE 'alterado_moderado'
  END,
  'Jaffé cinético compensado',
  re.data_emissao + INTERVAL '1 day',
  re.data_emissao + INTERVAL '2 days',
  (ARRAY['Lab CMPD Central','Lab CMPD Urgência','Lab CMPD Filial Norte'])[1+(re.requisicao_id%3)]
FROM laboratorio.requisicao_exame re
JOIN cadastro_beneficiario.perfil_saude ps ON ps.beneficiario_id = re.beneficiario_id
WHERE ps.numero_cpf BETWEEN '00000001001' AND '00000002500';

-- ============================================================
-- 7. laboratorio.achado_imagem — para 20% dos pacientes (~300 linhas)
-- ============================================================
INSERT INTO laboratorio.achado_imagem
  (requisicao_id, modalidade_imagem, regiao_anatomica,
   classificacao_bi_rads, descricao_radiologica, recomendacao,
   laudado_por, data_laudo_imagem)
SELECT
  re.requisicao_id,
  (ARRAY['ultrassonografia','tomografia','mamografia','ecocardiograma',
         'ressonancia','densitometria'])[1+(re.requisicao_id%6)],
  (ARRAY['abdome_total','torax','mama_bilateral','coracao',
         'coluna_lombar','quadril'])[1+(re.requisicao_id%6)],
  CASE WHEN (re.requisicao_id%6)=2 THEN 1 + (re.requisicao_id%5) ELSE NULL END,
  CASE re.requisicao_id % 6
    WHEN 0 THEN 'Ultrassonografia abdominal sem alterações significativas. Fígado de dimensões normais.'
    WHEN 1 THEN 'Tomografia de tórax: parênquima pulmonar preservado. Sem adenomegalias mediastinais.'
    WHEN 2 THEN 'Mamografia bilateral: padrão adiposo. Sem nódulos suspeitos ou microcalcificações.'
    WHEN 3 THEN 'Ecocardiograma: função sistólica preservada (FE 62%). Disfunção diastólica grau I.'
    WHEN 4 THEN 'Ressonância de coluna lombar: discreta protrusão discal L4-L5 sem compressão radicular.'
    ELSE        'Densitometria óssea: T-score coluna -1.2 (osteopenia leve). Quadril T-score -0.8.'
  END,
  CASE re.requisicao_id % 3
    WHEN 0 THEN 'Controle anual de rotina.'
    WHEN 1 THEN 'Repetir exame em 12 meses ou em caso de novos sintomas.'
    ELSE        'Avaliação complementar recomendada em 6 meses.'
  END,
  (ARRAY['CRM_RAD001','CRM_RAD002','CRM_RAD003'])[1+(re.requisicao_id%3)],
  re.data_emissao + INTERVAL '3 days'
FROM laboratorio.requisicao_exame re
JOIN cadastro_beneficiario.perfil_saude ps ON ps.beneficiario_id = re.beneficiario_id
WHERE ps.numero_cpf BETWEEN '00000001001' AND '00000002500'
  AND re.requisicao_id % 5 = 0;     -- 20% dos pacientes recebem laudo de imagem

SELECT 'CMPD PostgreSQL bulk seed concluído!' AS status;
