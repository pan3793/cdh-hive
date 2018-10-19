/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.messaging.json;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.codehaus.jackson.annotate.JsonProperty;

public class ExtendedJSONAlterDatabaseMessage extends JSONAlterDatabaseMessage {
  @JsonProperty
  private String newLocation;
  @JsonProperty
  private String oldLocation;
  @JsonProperty
  private String newDbName;
  @JsonProperty
  private String oldDbName;
  @JsonProperty
  private PrincipalType newOwnerType;
  @JsonProperty
  private PrincipalType oldOwnerType;
  @JsonProperty
  private String newOwnerName;
  @JsonProperty
  private String oldOwnerName;

  public ExtendedJSONAlterDatabaseMessage(String server, String servicePrincipal,
    Database before, Database after, Long timestamp) {
    super(server, servicePrincipal, before, after, timestamp);
    this.newLocation = before.getLocationUri();
    this.oldLocation = after.getLocationUri();
    this.oldDbName = before.getName();
    this.newDbName = after.getName();
    this.oldOwnerType = before.getOwnerType();
    this.newOwnerType = after.getOwnerType();
    this.oldOwnerName = before.getOwnerName();
    this.newOwnerName = after.getOwnerName();
  }

  public String getNewLocation() {
    return newLocation;
  }

  public String getOldLocation() {
    return oldLocation;
  }

  public String getOldDbName() {
    return oldDbName;
  }

  public String getNewDbName() {
    return newDbName;
  }

  public PrincipalType getNewOwnerType() {
    return newOwnerType;
  }

  public PrincipalType getOldOwnerType() {
    return oldOwnerType;
  }

  public String getNewOwnerName() {
    return newOwnerName;
  }

  public String getOldOwnerName() {
    return oldOwnerName;
  }

  @Override
  public String toString() {
    return ExtendedJSONMessageDeserializer.serialize(this);
  }
}
