-- Upgrade MetaStore schema from 1.1.0-cdh5.12.0 to 2.1.1-cdh6.0.0

UPDATE "APP".VERSION SET SCHEMA_VERSION='2.1.1', SCHEMA_VERSION_V2='2.1.1-cdh6.0.0', VERSION_COMMENT='Hive release version 2.1.1 for CDH 6.0.0' where VER_ID=1;
