-- Container bootstrap script.
--
-- Mounted into /docker-entrypoint-initdb.d/, so PostgreSQL runs it exactly
-- once, when the data volume is first created. It simply includes the same
-- schema and index files that `make db-init` applies, so the containerised
-- and manual paths can never drift apart.
--
-- Note: \i uses a container-internal path (see docker-compose.yml volumes).

\echo 'Creating heart_rate_readings schema...'
\i /docker-entrypoint-initdb.d/schema/create_tables.sql

\echo 'Creating indexes...'
\i /docker-entrypoint-initdb.d/indexes/create_indexes.sql

\echo 'Database initialisation complete.'
