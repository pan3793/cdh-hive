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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ant;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;

public class SetSystemProperty extends Task {

  private String propertyName;
  private String propertyValue;
 
  public void setPropertyName(String propertyName) {
    this.propertyName = propertyName;
  }
  public String getPropertyName() {
    return propertyName;
  }
  public void setPropertyValue(String propertyValue) {
    this.propertyValue = propertyValue;
  }
  public String getPropertyValue() {
    return propertyValue;
  }
  @Override
  public void execute() throws BuildException {
    if (propertyName == null) {
      throw new BuildException("No property name specified");
    }
    if (propertyValue == null) {
      System.clearProperty(propertyName);
    } else {
      System.setProperty(propertyName, propertyValue);
    }
  }
}
