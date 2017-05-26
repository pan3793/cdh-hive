SELECT 'Upgrading MetaStore schema from 1.1.0 to 2.1.1' AS Status from dual;

UPDATE VERSION SET SCHEMA_VERSION='2.1.1', SCHEMA_VERSION_V2='2.1.1-cdh6.0', VERSION_COMMENT='Hive release version 2.1.1 for CDH 6.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 1.1.0 to 2.1.1' AS Status from dual;
