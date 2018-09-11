package org.apache.hadoop.hive.metastore.messaging;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;

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
}
