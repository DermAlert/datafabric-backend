\set ON_ERROR_STOP on

-- This script is applied only to the temporary database created by
-- export-seed.sh. The source development database is never modified.

UPDATE core.data_connections
SET connection_params = (
        SELECT json_object_agg(
            entry.key,
            CASE
                WHEN entry.key ~* '(password|passwd|secret|token|api.?key|access.?key|credential)'
                    THEN to_json(
                        CASE core.data_connections.connection_params->>'host'
                            WHEN 'cmpd-postgres' THEN 'cmpd_pass'
                            WHEN 'ida-mysql' THEN 'ida_pass'
                            WHEN 'reh-postgres' THEN 'reh_pass'
                            WHEN 'cdi-postgres' THEN 'cdi_pass'
                            WHEN 'rfa-mysql' THEN 'rfa_pass'
                            WHEN 'dermexp_ham_postgres' THEN 'ham_pass'
                            WHEN 'dermexp_hiba_postgres' THEN 'hiba_pass'
                            WHEN 'dermexp_pad_mysql' THEN 'pad_pass'
                            ELSE 'change-me'
                        END::text
                    )
                ELSE entry.value
            END
        )
        FROM json_each(core.data_connections.connection_params) AS entry
    ),
    status = 'active',
    sync_status = 'idle',
    sync_progress = 0,
    sync_progress_details = NULL,
    next_sync_time = NULL;

UPDATE core.users
SET nome_usuario = 'Usuário de teste ' || id,
    email = 'usuario-' || id || '@example.test',
    cpf = lpad(id::text, 11, '0'),
    senha_hash = NULL,
    password_reset_token = NULL,
    password_reset_token_used = false,
    email_invite_token = NULL,
    email_invite_token_used = false;

UPDATE delta_sharing.recipients
SET identifier = 'recipient-' || id,
    name = 'Destinatário de teste ' || id,
    email = CASE WHEN email IS NULL THEN NULL ELSE 'recipient-' || id || '@example.test' END,
    organization_name = CASE WHEN organization_name IS NULL THEN NULL ELSE 'Organização de teste' END,
    bearer_token = 'dev-recipient-token-' || id,
    token_expiry = NULL,
    contact_info = NULL,
    notes = NULL;

UPDATE delta_sharing.shares
SET owner_email = CASE WHEN owner_email IS NULL THEN NULL ELSE 'owner@example.test' END,
    contact_info = NULL;

UPDATE delta_sharing.recipient_access_logs
SET request_query_params = NULL,
    request_body = NULL,
    request_path = '/seed/request',
    client_ip = CASE WHEN client_ip IS NULL THEN NULL ELSE '127.0.0.1' END,
    user_agent = CASE WHEN user_agent IS NULL THEN NULL ELSE 'datafabric-seed' END,
    error_message = NULL;

-- Source samples can contain names, identifiers, or clinical values. Keep the
-- catalog topology and types, but never include sampled source rows in git.
UPDATE metadata.external_columns
SET sample_values = '[]'::json,
    statistics = '{}'::json;

UPDATE datasets.bronze_executions
SET error_message = NULL,
    execution_details = NULL;

UPDATE datasets.silver_executions
SET error_message = NULL,
    execution_details = NULL;

UPDATE datasets.bronze_persistent_configs
SET last_execution_error = NULL;

UPDATE datasets.silver_persistent_configs
SET last_execution_error = NULL;

-- Guardrails: abort the export if a sensitive field was not anonymized.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM core.data_connections AS connection,
             LATERAL json_each_text(connection.connection_params) AS entry
        WHERE entry.key ~* '(password|passwd|secret|token|api.?key|access.?key|credential)'
          AND entry.value NOT IN (
              'cmpd_pass', 'ida_pass', 'reh_pass', 'cdi_pass', 'rfa_pass',
              'ham_pass', 'hiba_pass', 'pad_pass', 'change-me'
          )
    ) THEN
        RAISE EXCEPTION 'Sensitive connection parameters remain in seed database';
    END IF;

    IF EXISTS (
        SELECT 1 FROM delta_sharing.recipients
        WHERE email IS NOT NULL AND email !~ '@example[.]test$'
    ) THEN
        RAISE EXCEPTION 'Non-test recipient email remains in seed database';
    END IF;

    IF EXISTS (
        SELECT 1 FROM metadata.external_columns
        WHERE sample_values::jsonb <> '[]'::jsonb
           OR statistics::jsonb <> '{}'::jsonb
    ) THEN
        RAISE EXCEPTION 'Source samples remain in seed database';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM core.data_connections
        WHERE status <> 'active'
           OR (
                connection_params->>'host',
                connection_params->>'username',
                connection_params->>'password'
              ) NOT IN (
                ('cmpd-postgres', 'cmpd_user', 'cmpd_pass'),
                ('ida-mysql', 'ida_user', 'ida_pass'),
                ('reh-postgres', 'reh_user', 'reh_pass'),
                ('cdi-postgres', 'cdi_user', 'cdi_pass'),
                ('rfa-mysql', 'rfa_user', 'rfa_pass'),
                ('dermexp_ham_postgres', 'ham_user', 'ham_pass'),
                ('dermexp_hiba_postgres', 'hiba_user', 'hiba_pass'),
                ('dermexp_pad_mysql', 'pad_user', 'pad_pass')
              )
    ) THEN
        RAISE EXCEPTION 'Seed connection does not match a bundled Docker source';
    END IF;
END
$$;
