-- Upgrade MetaStore schema from 2.1.1-cdh6.1.0 to 2.1.1-cdh6.2.0

-- HIVE-21077
ALTER TABLE "APP"."DBS" ADD COLUMN CREATE_TIME INTEGER;

UPDATE "APP".CDH_VERSION SET SCHEMA_VERSION='2.1.1-cdh6.2.0', VERSION_COMMENT='Hive release version 2.1.1 for CDH 6.2.0' where VER_ID=1;
