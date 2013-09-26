-- Upgrade MetaStore schema from 0.11.0 to 0.11.0-c5b1
RUN '013-HIVE-3255.derby.sql';
RUN '014-HIVE-3764.derby.sql';
UPDATE "APP".VERSION SET SCHEMA_VERSION='0.11.0-c5b1', VERSION_COMMENT='Hive release version 0.11.0-c5b1' where VER_ID=1;
