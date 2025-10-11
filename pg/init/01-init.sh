#!/usr/bin/env bash
set -euo pipefail

# This script runs exactly once on first cluster init.
# Env vars come from docker-compose .env

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-SQL
  -- Create application database
  CREATE DATABASE ${TT_DB};

  -- Create app roles (login)
  CREATE ROLE ${TT_WRITER} LOGIN PASSWORD '${TT_WRITER_PASSWORD}';
  CREATE ROLE ${TT_READER} LOGIN PASSWORD '${TT_READER_PASSWORD}';

  -- Optional owner role
  DO \$\$
  BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tt_owner') THEN
      CREATE ROLE tt_owner NOLOGIN;
    END IF;
  END
  \$\$;

  ALTER DATABASE ${TT_DB} OWNER TO tt_owner;
SQL

# connect to app DB to configure privileges
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$TT_DB" <<-SQL
  ALTER SCHEMA public OWNER TO tt_owner;

  -- lock schema down
  REVOKE ALL ON SCHEMA public FROM PUBLIC;
  REVOKE CREATE ON SCHEMA public FROM PUBLIC;

  -- baseline privileges
  GRANT USAGE ON SCHEMA public TO ${TT_READER}, ${TT_WRITER};

  -- reader: SELECT only by default
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${TT_READER};
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ${TT_READER};

  -- writer: full DML
  GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ${TT_WRITER};
  ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ${TT_WRITER};
  ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO ${TT_WRITER};
SQL

echo "tick_trader DB and roles initialized."