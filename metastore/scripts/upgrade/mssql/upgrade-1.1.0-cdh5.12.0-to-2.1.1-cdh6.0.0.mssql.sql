SELECT 'Upgrading MetaStore schema from 1.1.0 to 2.1.1' AS MESSAGE;

-- upgrades schema from 1.1.0 to 1.2.0 excluding patches included in CDH-5.12.0
:r upgrade-1.1.0-to-1.2.0.mssql.sql

-- upgrades schema from 1.2.0 to 2.0.0 excluding patches included in CDH-5.12.0
:r upgrade-1.2.0-to-2.0.0.mssql.sql

-- upgrades schema from 2.0.0 to 2.1.0 excluding patches included in CDH-5.12.0
:r upgrade-2.0.0-to-2.1.0.mssql.sql

-- Apply incremental schema changes to the 2.1.0 schema
:r 024-HIVE-12274.mssql.sql
:r 032-HIVE-18202.mssql.sql
:r 018-HIVE-6757.mssql.sql

UPDATE VERSION SET SCHEMA_VERSION='2.1.1', SCHEMA_VERSION_V2='2.1.1-cdh6.0.0', VERSION_COMMENT='Hive release version 2.1.1 for CDH 6.0.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 1.1.0 to 2.1.1' AS MESSAGE;
