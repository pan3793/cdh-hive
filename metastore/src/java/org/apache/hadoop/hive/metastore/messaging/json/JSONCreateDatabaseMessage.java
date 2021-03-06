/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.messaging.json;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.messaging.CreateDatabaseMessage;
import org.apache.thrift.TException;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * JSON Implementation of CreateDatabaseMessage.
 */
public class JSONCreateDatabaseMessage extends CreateDatabaseMessage {

  @JsonProperty
  String server, servicePrincipal, db, dbJson;

  @JsonProperty
  Long timestamp;

  /**
   * Default constructor, required for Jackson.
   */
  public JSONCreateDatabaseMessage() {}

  public JSONCreateDatabaseMessage(String server, String servicePrincipal, String dbname,
      Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.db = dbname;
    this.timestamp = timestamp;
    checkValid();
  }

  public JSONCreateDatabaseMessage(String server, String servicePrincipal, Database db,
      Long timestamp) {
    this(server, servicePrincipal, db.getName(), timestamp);
    try {
      this.dbJson = JSONMessageFactory.createDatabaseObjJson(db);
    } catch (TException ex) {
      throw new IllegalArgumentException("Could not serialize Function object", ex);
    }
  }

  public Database getDatabaseObject() throws Exception {
    return (Database) JSONMessageFactory.getTObj(dbJson, Database.class);
  }

  @Override
  public String getDB() { return db; }

  @Override
  public String getServer() { return server; }

  @Override
  public String getServicePrincipal() { return servicePrincipal; }

  @Override
  public Long getTimestamp() { return timestamp; }

  @Override
  public String toString() {
    try {
      return JSONMessageDeserializer.mapper.writeValueAsString(this);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not serialize: ", exception);
    }
  }

}
