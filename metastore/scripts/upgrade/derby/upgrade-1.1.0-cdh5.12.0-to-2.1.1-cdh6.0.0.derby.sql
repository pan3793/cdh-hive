-- Upgrade MetaStore schema from 1.1.0-cdh5.12.0 to 2.1.1-cdh6.0.0

-- Run schema patches to upgrade schema from 1.1.0-to-2.0.0 excluding the patches already applied in CDH-5.12.0
RUN 'upgrade-1.1.0-to-1.2.0.derby.sql';
RUN 'upgrade-1.2.0-to-2.0.0.derby.sql';

-- Run schema patches to upgrade schema from 1.1.0-to-2.1.1 excluding the patches already applied in CDH-5.12.0
RUN 'upgrade-2.0.0-to-2.1.0.derby.sql';

-- Apply incremental schema changes to the 2.1.0 schema
RUN '039-HIVE-12274.derby.sql';
RUN '018-HIVE-6757.derby.sql';
RUN '049-HIVE-18489.derby.sql';

UPDATE "APP".VERSION SET SCHEMA_VERSION='2.1.1', SCHEMA_VERSION_V2='2.1.1-cdh6.0.0', VERSION_COMMENT='Hive release version 2.1.1 for CDH 6.0.0' where VER_ID=1;
