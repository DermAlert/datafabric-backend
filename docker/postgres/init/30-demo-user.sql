\set ON_ERROR_STOP on

-- Reusable local-only account for exercising authenticated API endpoints.
-- The snapshot sanitizer removes every original password before this file runs.
-- PostgreSQL's reserved .test domain is intentionally anonymized, but Pydantic's
-- EmailStr rejects it, so use the equally fictitious and validator-safe example.com.
UPDATE core.users
SET email = regexp_replace(email, '@example[.]test$', '@example.com')
WHERE email ~ '@example[.]test$';

UPDATE delta_sharing.recipients
SET email = regexp_replace(email, '@example[.]test$', '@example.com')
WHERE email ~ '@example[.]test$';

UPDATE delta_sharing.shares
SET owner_email = regexp_replace(owner_email, '@example[.]test$', '@example.com')
WHERE owner_email ~ '@example[.]test$';

INSERT INTO core.roles (name, nivel_acesso)
VALUES ('Administrador', 0)
ON CONFLICT (name) DO UPDATE
SET nivel_acesso = EXCLUDED.nivel_acesso;

INSERT INTO core.users (
    organization_id,
    nome_usuario,
    email,
    cpf,
    senha_hash,
    password_reset_token_used,
    email_invite_token_used,
    fl_ativo
)
VALUES (
    (SELECT id FROM core.organizacoes ORDER BY id LIMIT 1),
    'Administrador de demonstração',
    'admin@example.com',
    '12345678901',
    '$2b$12$oklUjzY9dz/j3xWmwslkmOckmhe40mS6PIHgWAbHO5gjZoUaqLJxC',
    false,
    false,
    true
)
ON CONFLICT (cpf) DO UPDATE
SET organization_id = EXCLUDED.organization_id,
    nome_usuario = EXCLUDED.nome_usuario,
    email = EXCLUDED.email,
    senha_hash = EXCLUDED.senha_hash,
    password_reset_token = NULL,
    password_reset_token_used = false,
    email_invite_token = NULL,
    email_invite_token_used = false,
    fl_ativo = true;

INSERT INTO core.user_roles (user_id, role_id)
SELECT users.id, roles.id
FROM core.users AS users
CROSS JOIN core.roles AS roles
WHERE users.cpf = '12345678901'
  AND roles.name = 'Administrador'
  AND NOT EXISTS (
      SELECT 1
      FROM core.user_roles AS existing
      WHERE existing.user_id = users.id
        AND existing.role_id = roles.id
  );
