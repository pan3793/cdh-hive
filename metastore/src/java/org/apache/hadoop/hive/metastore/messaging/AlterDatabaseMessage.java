package org.apache.hadoop.hive.metastore.messaging;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.api.Database;

/**
 * AlterDatabaseMessage.
 * Abstract class to store the Alter database message
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class AlterDatabaseMessage extends EventMessage {
  protected AlterDatabaseMessage() {
    super(EventType.ALTER_DATABASE);
  }

  public abstract Database getDbObjBefore() throws Exception;
  public abstract Database getDbObjAfter() throws Exception;
}
