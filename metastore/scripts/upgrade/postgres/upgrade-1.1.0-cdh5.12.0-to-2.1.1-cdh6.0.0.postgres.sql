SELECT 'Upgrading MetaStore schema from 1.1.0 to 2.1.1';

-- upgrading schema from 1.1.0 to 2.0.0 excluding patches already applied in CDH-5.12.0. There are no patches required to upgrade from 1.1.0-to-1.2.0
\i upgrade-1.1.0-to-1.2.0.postgres.sql;
\i upgrade-1.2.0-to-2.0.0.postgres.sql;

-- upgrading schema from 2.0.0 to 2.1.0 excluding patches already applied in CDH-5.12.0
\i upgrade-2.0.0-to-2.1.0.postgres.sql;

-- Apply incremental schema changes to the 2.1.0 schema
\i 038-HIVE-12274.postgres.sql;

UPDATE "VERSION" SET "SCHEMA_VERSION"='1.1.0', "SCHEMA_VERSION_V2"='2.1.1-cdh6.0.0', "VERSION_COMMENT"='Hive release version 2.1.1 for CDH 6.0.0' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 1.1.0 to 2.1.1';
