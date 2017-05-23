-- Upgrade MetaStore schema from 1.1.0 to 2.1.1
RUN '041-HIVE-16556.derby.sql';

UPDATE "APP".VERSION SET SCHEMA_VERSION='2.1.1-cdh6.0', VERSION_COMMENT='Hive release version 2.1.1 for CDH 6.0' where VER_ID=1;
