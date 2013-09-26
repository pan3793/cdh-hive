SELECT 'Upgrading MetaStore schema from 0.11.0 to 0.11.0-c5b1' AS ' ';
SOURCE 013-HIVE-3255.mysql.sql;
SOURCE 014-HIVE-3764.mysql.sql;
UPDATE VERSION SET SCHEMA_VERSION='0.11.0-c5b1', VERSION_COMMENT='Hive release version 0.11.0-c5b1' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 0.11.0 to 0.11.0-c5b1' AS ' ';
