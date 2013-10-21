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

package org.apache.hive.service.cli.session;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.log.LogManager;
import org.apache.hive.service.cli.operation.OperationManager;

/**
 * SessionManager.
 *
 */
public class SessionManager extends CompositeService {
  private static final Log LOG = LogFactory.getLog(CompositeService.class);
  private HiveConf hiveConf;
  private final Map<SessionHandle, HiveSession> handleToSession = new HashMap<SessionHandle, HiveSession>();
  private OperationManager operationManager = new OperationManager();
  private LogManager logManager = new LogManager();
  private static final Object sessionMapLock = new Object();
  private ExecutorService backgroundOperationPool;

  public SessionManager() {
    super("SessionManager");
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    operationManager = new OperationManager();
    int backgroundPoolSize = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_THREADS);
    LOG.info("HiveServer2: Async execution pool size" + backgroundPoolSize);
    backgroundOperationPool = Executors.newFixedThreadPool(backgroundPoolSize);
    addService(operationManager);
    logManager = new LogManager();
    logManager.setSessionManager(this);

    addService(logManager);
    super.init(hiveConf);
  }

  @Override
  public synchronized void start() {
    super.start();
    // TODO
  }

  @Override
  public synchronized void stop() {
    // TODO
    super.stop();
    if (backgroundOperationPool != null) {
      backgroundOperationPool.shutdown();
      long timeout = hiveConf.getLongVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT);
      try {
        backgroundOperationPool.awaitTermination(timeout, TimeUnit.SECONDS);
      } catch (InterruptedException exc) {
        LOG.warn("HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT = " + timeout +
        		" seconds has been exceeded. RUNNING background operations will be shut down", exc);
      }
    }
  }


  public SessionHandle openSession(String username, String password, Map<String, String> sessionConf)
          throws HiveSQLException {
     return openSession(username, password, sessionConf, false, null);
  }

  public SessionHandle openSession(String username, String password, Map<String, String> sessionConf,
          boolean withImpersonation, String delegationToken) throws HiveSQLException {
    HiveSession session;
    if (username == null) {
      username = threadLocalUserName.get();
    }
    username = getProxyUser(username, sessionConf, threadLocalIpAddress.get());

    if (withImpersonation) {
          HiveSessionImplwithUGI hiveSessionUgi = new HiveSessionImplwithUGI(username, password, sessionConf,
              threadLocalIpAddress.get(), delegationToken);
          session = (HiveSession)HiveSessionProxy.getProxy(hiveSessionUgi, hiveSessionUgi.getSessionUgi());
          hiveSessionUgi.setProxySession(session);
    } else {
      session = new HiveSessionImpl(username, password, sessionConf, threadLocalIpAddress.get());
    }
    session.setSessionManager(this);
    session.setOperationManager(operationManager);
    session.setLogManager(logManager);
    synchronized(sessionMapLock) {
      handleToSession.put(session.getSessionHandle(), session);
    }
    try {
      executeSessionHooks(session);
    } catch (Exception e) {
      throw new HiveSQLException("Failed to execute session hooks", e);
    }
    return session.getSessionHandle();
  }

  public void closeSession(SessionHandle sessionHandle) throws HiveSQLException {
    HiveSession session;
    synchronized(sessionMapLock) {
      session = handleToSession.remove(sessionHandle);
    }
    if (session == null) {
      throw new HiveSQLException("Session does not exist!");
    }
    session.close();
  }


  public HiveSession getSession(SessionHandle sessionHandle) throws HiveSQLException {
    HiveSession session;
    synchronized(sessionMapLock) {
      session = handleToSession.get(sessionHandle);
    }
    if (session == null) {
      throw new HiveSQLException("Invalid SessionHandle: " + sessionHandle);
    }
    return session;
  }

  public OperationManager getOperationManager() {
    return operationManager;
  }

  public LogManager getLogManager() {
    return logManager;
  }

  private static ThreadLocal<String> threadLocalIpAddress = new ThreadLocal<String>() {
    @Override
    protected synchronized String initialValue() {
      return null;
    }
  };

  public static void setIpAddress(String ipAddress) {
    threadLocalIpAddress.set(ipAddress);
  }

  public static void clearIpAddress() {
    threadLocalIpAddress.remove();
  }

  private static ThreadLocal<String> threadLocalUserName = new ThreadLocal<String>(){
    @Override
    protected synchronized String initialValue() {
      return null;
    }
  };

  public static void setUserName(String userName) {
    threadLocalUserName.set(userName);
  }

  private void clearUserName() {
    threadLocalUserName.remove();
  }


  // execute session hooks
  private void executeSessionHooks(HiveSession session) throws Exception {
    String hookList = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_SESSION_HOOK);
    if (hookList == null || hookList.trim().isEmpty()) {
      return;
    }

    String[] hookClasses = hookList.trim().split(",");
    for (String hookClass : hookClasses) {
      HiveSessionHook sessionHook = (HiveSessionHook)
          Class.forName(hookClass.trim(), true, JavaUtils.getClassLoader()).newInstance();
      sessionHook.run(new HiveSessionHookContextImpl(session));
    }
  }

  public Future<?> submitBackgroundOperation(Callable<Void> backgroundOperation) {
    return backgroundOperationPool.submit(backgroundOperation);
  }

  /**
   * If the proxy user name is provided then check privileges to substitute the user.
   * @param realUser
   * @param sessionConf
   * @param ipAddress
   * @return
   * @throws HiveSQLException
   */
  private String getProxyUser(String realUser, Map<String, String> sessionConf, String ipAddress)
      throws HiveSQLException {
    if (sessionConf == null || !sessionConf.containsKey(HiveAuthFactory.HS2_PROXY_USER)) {
      return realUser;
    }

    // Extract the proxy user name and check if we are allowed to do the substitution
    String proxyUser = sessionConf.get(HiveAuthFactory.HS2_PROXY_USER);
    if (!hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ALLOW_USER_SUBSTITUTION)) {
      throw new HiveSQLException("Proxy user substitution is not allowed");
    }

    // If there's no authentication, then directly substitute the user
    if (HiveAuthFactory.AuthTypes.NONE.toString().
        equalsIgnoreCase(hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION))) {
      return proxyUser;
    }

    // Verify proxy user privilege of the realUser for the proxyUser
    try {
      UserGroupInformation sessionUgi;
      if (!ShimLoader.getHadoopShims().isSecurityEnabled()) {
        sessionUgi = ShimLoader.getHadoopShims().createProxyUser(realUser);
      } else {
        sessionUgi = ShimLoader.getHadoopShims().createRemoteUser(realUser, null);
      }
      ShimLoader.getHadoopShims().
          authorizeProxyAccess(proxyUser, sessionUgi, ipAddress, hiveConf);
      return proxyUser;
    } catch (IOException e) {
      throw new HiveSQLException("Failed to validate proxy privilage of " + realUser +
          " for " + proxyUser, e);
    }
  }
}

