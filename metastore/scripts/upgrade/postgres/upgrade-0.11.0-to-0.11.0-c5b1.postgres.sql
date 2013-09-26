SELECT 'Upgrading MetaStore schema from 0.11.0 to 0.11.0-c5b1';
\i 013-HIVE-3255.postgres.sql;
\i 014-HIVE-3764.postgres.sql;
UPDATE VERSION SET SCHEMA_VERSION='0.11.0-c5b1', VERSION_COMMENT='Hive release version 0.11.0-c5b1' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 0.11.0 to 0.11.0-c5b1';
