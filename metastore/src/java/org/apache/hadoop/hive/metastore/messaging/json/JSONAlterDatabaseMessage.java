package org.apache.hadoop.hive.metastore.messaging.json;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.messaging.AlterDatabaseMessage;
import org.apache.thrift.TException;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * JSON alter database message.
 */
public class JSONAlterDatabaseMessage extends AlterDatabaseMessage {

  @JsonProperty
  String server, servicePrincipal, db, dbObjBeforeJson, dbObjAfterJson;

  @JsonProperty
  Long timestamp;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONAlterDatabaseMessage() {
  }

  public JSONAlterDatabaseMessage(String server, String servicePrincipal,
      Database dbObjBefore, Database dbObjAfter, Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.db = dbObjBefore.getName();
    this.timestamp = timestamp;
    try {
      this.dbObjBeforeJson = JSONMessageFactory.createDatabaseObjJson(dbObjBefore);
      this.dbObjAfterJson = JSONMessageFactory.createDatabaseObjJson(dbObjAfter);
    } catch (TException e) {
      throw new IllegalArgumentException("Could not serialize: ", e);
    }
    checkValid();
  }

  @Override
  public String getServer() {
    return server;
  }

  @Override
  public String getServicePrincipal() {
    return servicePrincipal;
  }

  @Override
  public String getDB() {
    return db;
  }

  @Override
  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public Database getDbObjBefore() throws Exception {
    return (Database) JSONMessageFactory.getTObj(dbObjBeforeJson, Database.class);
  }

  @Override
  public Database getDbObjAfter() throws Exception {
    return (Database) JSONMessageFactory.getTObj(dbObjAfterJson, Database.class);
  }

  @Override
  public String toString() {
    try {
      return JSONMessageDeserializer.mapper.writeValueAsString(this);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not serialize: ", e);
    }
  }
}
