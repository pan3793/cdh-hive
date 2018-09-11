package org.apache.hadoop.hive.metastore.messaging.json;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.messaging.AlterDatabaseMessage;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * JSON alter database message.
 */
public class JSONAlterDatabaseMessage extends AlterDatabaseMessage {

  @JsonProperty
  String server, servicePrincipal, db;

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
  public String toString() {
    try {
      return JSONMessageDeserializer.mapper.writeValueAsString(this);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not serialize: ", e);
    }
  }
}
