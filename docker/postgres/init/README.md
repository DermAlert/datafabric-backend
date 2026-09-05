# PostgreSQL development snapshot

`20-demo-data.sql.gz` is a full, anonymized snapshot of the application
database. The official PostgreSQL image restores it automatically when it
initializes an empty `backend_pgdata` volume.

`30-demo-user.sql` then creates an active local administrator for API tests:
CPF `12345678901`, password `123456`. This account contains no production
identity or credential and is intended only for this Docker Compose stack.

Do not place raw database dumps in this directory. Refresh the snapshot with
`../export-seed.sh`, which clones and sanitizes the source database first.
