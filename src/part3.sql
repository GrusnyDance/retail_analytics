CREATE ROLE Visitor WITH LOGIN PASSWORD '1234'
NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION VALID UNTIL 'infinity';

GRANT CONNECT ON DATABASE postgres TO Visitor;
GRANT USAGE ON SCHEMA public TO Visitor;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO Visitor;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO Visitor;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO Visitor;


CREATE ROLE Administrator WITH LOGIN PASSWORD '1234'
NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION VALID UNTIL 'infinity';
--GRANT ALL ON SCHEMA public TO Administrator;
GRANT CONNECT ON DATABASE postgres TO Administrator;
GRANT USAGE ON SCHEMA public TO Administrator;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO Administrator;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO Administrator;
GRANT INSERT ON ALL TABLES IN SCHEMA public TO Administrator;


--DROP OWNED BY
-- \conninfo

-- create table loh (
-- 	name varchar(30);

-- INSERT INTO loh (name) VALUES ('petya1');