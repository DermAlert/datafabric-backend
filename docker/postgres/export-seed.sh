#!/usr/bin/env bash
set -Eeuo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly SOURCE_DATABASE="dermalert_backend"
readonly SNAPSHOT_DATABASE="datafabric_seed_export_tmp"
readonly POSTGRES_SERVICE="postgres-backend"
readonly OUTPUT_FILE="${ROOT_DIR}/docker/postgres/init/20-demo-data.sql.gz"
readonly SANITIZE_FILE="${ROOT_DIR}/docker/postgres/sanitize-seed.sql"

compose() {
    docker compose --project-directory "${ROOT_DIR}" "$@"
}

psql_admin() {
    compose exec -T "${POSTGRES_SERVICE}" \
        psql --username postgres --dbname postgres --set ON_ERROR_STOP=on "$@"
}

cleanup() {
    psql_admin --command \
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${SNAPSHOT_DATABASE}' AND pid <> pg_backend_pid();" \
        >/dev/null 2>&1 || true
    psql_admin --command "DROP DATABASE IF EXISTS ${SNAPSHOT_DATABASE};" >/dev/null 2>&1 || true
}

trap cleanup EXIT

if ! compose ps --status running --services | grep -qx "${POSTGRES_SERVICE}"; then
    echo "Starting ${POSTGRES_SERVICE}..."
    compose up -d --wait "${POSTGRES_SERVICE}"
fi

mkdir -p "$(dirname "${OUTPUT_FILE}")"

cleanup
psql_admin --command "CREATE DATABASE ${SNAPSHOT_DATABASE} TEMPLATE ${SOURCE_DATABASE};"

compose exec -T "${POSTGRES_SERVICE}" \
    psql --username postgres --dbname "${SNAPSHOT_DATABASE}" --set ON_ERROR_STOP=on \
    < "${SANITIZE_FILE}"

temporary_dump="$(mktemp "${OUTPUT_FILE}.tmp.XXXXXX")"
trap 'rm -f "${temporary_dump}"; cleanup' EXIT

compose exec -T "${POSTGRES_SERVICE}" \
    pg_dump --username postgres --dbname "${SNAPSHOT_DATABASE}" \
        --format plain --no-owner --no-privileges \
    | sed -E \
        -e 's/^\\restrict .*/\\restrict datafabric_seed_snapshot/' \
        -e 's/^\\unrestrict .*/\\unrestrict datafabric_seed_snapshot/' \
    | gzip --no-name --best > "${temporary_dump}"

gzip --test "${temporary_dump}"
if gzip --decompress --stdout "${temporary_dump}" | grep -Eiq \
    '(BEGIN (RSA |OPENSSH )?PRIVATE KEY|AKIA[0-9A-Z]{16})'; then
    echo "Refusing to write snapshot: a private key or AWS access key pattern was found." >&2
    exit 1
fi

mv "${temporary_dump}" "${OUTPUT_FILE}"
chmod 0644 "${OUTPUT_FILE}"
trap cleanup EXIT

echo "Anonymized snapshot written to ${OUTPUT_FILE}"
