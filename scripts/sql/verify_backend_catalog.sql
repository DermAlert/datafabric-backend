WITH demo_tables AS (
    SELECT
        t.id AS table_id,
        t.table_name,
        c.name AS source_name
    FROM metadata.external_tables t
    JOIN core.data_connections c ON c.id = t.connection_id
    WHERE (c.name, t.table_name) IN (
        ('IDA Synthetic Demo', 'ficha_dermato'),
        ('CMPD Synthetic Demo', 'perfil_saude')
    )
), mapping_counts AS (
    SELECT
        d.source_name,
        d.table_name,
        COUNT(DISTINCT cm.id) AS column_mappings,
        COUNT(DISTINCT cm.id) FILTER (WHERE cm.fl_ativo) AS active_column_mappings,
        COUNT(DISTINCT vm.id) AS value_mappings,
        COUNT(DISTINCT vm.id) FILTER (WHERE vm.fl_ativo) AS active_value_mappings
    FROM demo_tables d
    LEFT JOIN metadata.external_columns ec ON ec.table_id = d.table_id
    LEFT JOIN equivalence.column_mappings cm
        ON cm.column_id = ec.id
        AND cm.group_id = (
            SELECT id FROM equivalence.column_groups
            WHERE name = 'sexo_biologico_padronizado'
        )
    LEFT JOIN equivalence.value_mappings vm
        ON vm.source_column_id = ec.id
        AND vm.group_id = (
            SELECT id FROM equivalence.column_groups
            WHERE name = 'sexo_biologico_padronizado'
        )
    GROUP BY d.source_name, d.table_name
)
SELECT
    d.source_name,
    d.table_name,
    COUNT(ec.id) AS catalog_columns,
    COUNT(ec.id) FILTER (WHERE ec.fl_ativo) AS exposed_columns,
    m.column_mappings,
    m.active_column_mappings,
    m.value_mappings,
    m.active_value_mappings
FROM demo_tables d
JOIN metadata.external_columns ec ON ec.table_id = d.table_id
JOIN mapping_counts m USING (source_name, table_name)
GROUP BY
    d.source_name,
    d.table_name,
    m.column_mappings,
    m.active_column_mappings,
    m.value_mappings,
    m.active_value_mappings
ORDER BY d.source_name;

SELECT
    c.name AS source_name,
    t.table_name,
    ec.column_name,
    vm.source_value,
    vm.standard_value,
    vm.record_count,
    vm.fl_ativo
FROM equivalence.value_mappings vm
JOIN metadata.external_columns ec ON ec.id = vm.source_column_id
JOIN metadata.external_tables t ON t.id = ec.table_id
JOIN core.data_connections c ON c.id = t.connection_id
WHERE vm.group_id = (
    SELECT id FROM equivalence.column_groups
    WHERE name = 'sexo_biologico_padronizado'
)
ORDER BY vm.id;

SELECT
    c.name AS source_name,
    t.table_name,
    ec.column_name,
    cm.transformation_rule,
    cm.confidence_score,
    cm.fl_ativo
FROM equivalence.column_mappings cm
JOIN metadata.external_columns ec ON ec.id = cm.column_id
JOIN metadata.external_tables t ON t.id = ec.table_id
JOIN core.data_connections c ON c.id = t.connection_id
WHERE cm.group_id = (
    SELECT id FROM equivalence.column_groups
    WHERE name = 'sexo_biologico_padronizado'
)
ORDER BY cm.id;

SELECT
    COUNT(*) AS native_approval_or_conflict_fields
FROM information_schema.columns
WHERE table_schema = 'equivalence'
  AND table_name IN ('column_mappings', 'value_mappings')
  AND (
      column_name ILIKE '%approv%'
      OR column_name ILIKE '%conflict%'
      OR column_name ILIKE '%status%'
  );

SELECT
    COUNT(*) AS native_semantic_version_fields
FROM information_schema.columns
WHERE table_schema = 'equivalence'
  AND column_name ILIKE '%version%';

SELECT
    id,
    name,
    left_table_id,
    left_column_id,
    right_table_id,
    right_column_id,
    cardinality,
    default_join_type,
    is_verified,
    is_active
FROM metadata.table_relationships
WHERE name = 'dermalert_demo_cpf_join';

SELECT
    e.id,
    status,
    rows_ingested,
    version_number,
    path_delta_versions,
    write_mode_used
FROM datasets.bronze_executions e
JOIN datasets.bronze_persistent_configs c ON c.id = e.config_id
WHERE c.name = 'dermalert_demo_bronze'
  AND e.status = 'SUCCESS'
ORDER BY e.started_at DESC
LIMIT 1;

SELECT
    e.id,
    status,
    rows_processed,
    rows_output,
    delta_version,
    write_mode_used,
    execution_details
FROM datasets.silver_executions e
JOIN datasets.silver_persistent_configs c ON c.id = e.config_id
WHERE c.name = 'dermalert_demo_silver'
  AND e.status = 'SUCCESS'
ORDER BY e.started_at DESC
LIMIT 1;

SELECT COUNT(*) AS persisted_bronze_column_lineage_rows
FROM datasets.bronze_column_mappings
WHERE external_table_id IN (
    SELECT t.id
    FROM metadata.external_tables t
    JOIN core.data_connections c ON c.id = t.connection_id
    WHERE (c.name, t.table_name) IN (
        ('IDA Synthetic Demo', 'ficha_dermato'),
        ('CMPD Synthetic Demo', 'perfil_saude')
    )
);
