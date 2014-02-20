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
package org.apache.hadoop.hive.ql.parse.authorization;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;

/**
 * Simple factory class to create the HiveAuthorizationTaskFactory
 * based on mocking. The create method is not static so we can easily
 * mock this class if required.
 */
public class HiveAuthorizationTaskFactoryFactory {
  protected final HiveConf conf;
  protected final Hive db;
  public HiveAuthorizationTaskFactoryFactory(HiveConf conf, Hive db) {
    this.conf = conf;
    this.db = db;
  }
  public HiveAuthorizationTaskFactory create() {
    Class<? extends HiveAuthorizationTaskFactory> authProviderClass = conf.
        getClass(HiveConf.ConfVars.HIVE_AUTHORIZATION_TASK_FACTORY.varname,
            HiveAuthorizationTaskFactoryImpl.class,
            HiveAuthorizationTaskFactory.class);
    String msg = "Unable to create instance of " + authProviderClass.getName() + ": ";
    try {
      Constructor<? extends HiveAuthorizationTaskFactory> constructor =
          authProviderClass.getConstructor(HiveConf.class, Hive.class);
      return constructor.newInstance(conf, db);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
    } catch (SecurityException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
    } catch (InstantiationException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
    } catch (InvocationTargetException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
    }
  }
}
