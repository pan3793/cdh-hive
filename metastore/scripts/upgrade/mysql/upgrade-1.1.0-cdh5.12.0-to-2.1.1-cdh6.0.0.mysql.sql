SELECT 'Upgrading MetaStore schema from 1.1.0 to 2.1.1' AS ' ';

-- upgrades the schema from 1.1.0 to 2.0.0 excluding CDH patches which are already applied in CDH-5.12.0
SOURCE upgrade-1.1.0-to-1.2.0.mysql.sql;
SOURCE upgrade-1.2.0-to-2.0.0.mysql.sql;

-- upgrades the schema from 2.0.0 to 2.1.0 excluding CDH patches which are already applied in CDH-5.12.0
SOURCE upgrade-2.0.0-to-2.1.0.mysql.sql;

-- Apply incremental schema changes to the 2.1.0 schema
SOURCE 039-HIVE-12274.mysql.sql;
SOURCE 047-HIVE-18202.mysql.sql;
SOURCE 018-HIVE-6757.mysql.sql;

UPDATE VERSION SET SCHEMA_VERSION='2.1.1', SCHEMA_VERSION_V2='2.1.1-cdh6.0.0', VERSION_COMMENT='Hive release version 2.1.1 for CDH 6.0.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 1.1.0 to 2.1.1' AS ' ';
