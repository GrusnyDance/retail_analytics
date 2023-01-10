SELECT * FROM pg_roles where left(rolname, 2) <> 'pg';

--DROP OWNED BY visitor;
DROP ROLE IF EXISTS visitor;
CREATE ROLE visitor WITH LOGIN
NOSUPERUSER NOCREATEDB NOCREATEROLE;

GRANT CONNECT ON DATABASE postgres TO visitor;
GRANT USAGE ON SCHEMA public TO visitor;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO visitor;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO visitor;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO visitor;

--DROP OWNED BY administrator;
DROP ROLE IF EXISTS administrator;
CREATE ROLE administrator WITH LOGIN
SUPERUSER;

SELECT * FROM pg_roles where left(rolname, 2) <> 'pg';


-- \conninfo

-- create table students (name varchar(30);
-- INSERT INTO students (name) VALUES ('petya');
