CREATE TABLE test_uuid (key STRING, uid STRING);

INSERT INTO TABLE test_uuid SELECT key, uuid() FROM src;

SELECT COUNT(DISTINCT uid) FROM test_uuid;
