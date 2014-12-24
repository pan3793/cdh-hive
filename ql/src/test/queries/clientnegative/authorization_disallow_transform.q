set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authorization.enabled=true;
set hive.entity.capture.transform=true;
set role ALL;
SELECT TRANSFORM (*) USING 'cat' AS (key, value) FROM src;
