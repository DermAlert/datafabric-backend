# PostgreSQL development snapshot

`20-demo-data.sql.gz` is a full, anonymized snapshot of the application
database. The official PostgreSQL image restores it automatically when it
initializes an empty `backend_pgdata` volume.

Do not place raw database dumps in this directory. Refresh the snapshot with
`../export-seed.sh`, which clones and sanitizes the source database first.
