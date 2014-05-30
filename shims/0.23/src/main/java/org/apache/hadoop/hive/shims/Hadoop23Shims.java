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
package org.apache.hadoop.hive.shims;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ProxyFileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.WebHCatJTShim23;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.QueuePlacementPolicy;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * Implemention of shims against Hadoop 0.23.0.
 */
public class Hadoop23Shims extends HadoopShimsSecure {
  private static final String MR2_JOB_QUEUE_PROPERTY = "mapreduce.job.queuename";

  @Override
  public String getTaskAttemptLogUrl(JobConf conf,
    String taskTrackerHttpAddress, String taskAttemptId)
    throws MalformedURLException {
    if (isMR2(conf)) {
      // if the cluster is running in MR2 mode, return null
      LOG.warn("Can't fetch tasklog: TaskLogServlet is not supported in MR2 mode.");
      return null;
    } else {
      // MR2 doesn't have TaskLogServlet class, so need to
      String taskLogURL = null;
      try {
        Class<?> taskLogClass= Class.forName("TaskLogServlet");
        Method taskLogMethod  = taskLogClass.getDeclaredMethod("getTaskLogUrl", String.class, String.class, String.class);
        URL taskTrackerHttpURL = new URL(taskTrackerHttpAddress);
        taskLogURL = (String)taskLogMethod.invoke(null, taskTrackerHttpURL.getHost(),
            Integer.toString(taskTrackerHttpURL.getPort()), taskAttemptId);
      } catch (IllegalArgumentException e) {
        LOG.error("Error trying to get task log URL", e);
        throw new MalformedURLException("Could not execute getTaskLogUrl: " + e.getCause());
      } catch (IllegalAccessException e) {
        LOG.error("Error trying to get task log URL", e);
        throw new MalformedURLException("Could not execute getTaskLogUrl: " + e.getCause());
      } catch (InvocationTargetException e) {
        LOG.error("Error trying to get task log URL", e);
        throw new MalformedURLException("Could not execute getTaskLogUrl: " + e.getCause());
      } catch (SecurityException e) {
        LOG.error("Error trying to get task log URL", e);
        throw new MalformedURLException("Could not execute getTaskLogUrl: " + e.getCause());
      } catch (NoSuchMethodException e) {
        LOG.error("Error trying to get task log URL", e);
        throw new MalformedURLException("Method getTaskLogUrl not found: " + e.getCause());
      } catch (ClassNotFoundException e) {
        LOG.warn("Can't fetch tasklog: TaskLogServlet is not supported in MR2 mode.");
      }
      return taskLogURL;
    }
  }

  @Override
  public JobTrackerState getJobTrackerState(ClusterStatus clusterStatus) throws Exception {
    switch (clusterStatus.getJobTrackerStatus()) {
    case INITIALIZING:
      return JobTrackerState.INITIALIZING;
    case RUNNING:
      return JobTrackerState.RUNNING;
    default:
      String errorMsg = "Unrecognized JobTracker state: " + clusterStatus.getJobTrackerStatus();
      throw new Exception(errorMsg);
    }
  }

  @Override
  public org.apache.hadoop.mapreduce.TaskAttemptContext newTaskAttemptContext(Configuration conf, final Progressable progressable) {
    return new TaskAttemptContextImpl(conf, new TaskAttemptID()) {
      @Override
      public void progress() {
        progressable.progress();
      }
    };
  }

  @Override
  public TaskAttemptID newTaskAttemptID(JobID jobId, boolean isMap, int taskId, int id) {
    return new TaskAttemptID(jobId.getJtIdentifier(), jobId.getId(), isMap ?  TaskType.MAP : TaskType.REDUCE, taskId, id);
  }

  @Override
  public org.apache.hadoop.mapreduce.JobContext newJobContext(Job job) {
    return new JobContextImpl(job.getConfiguration(), job.getJobID());
  }

  @Override
  public boolean isLocalMode(Configuration conf) {
    if (isMR2(conf)) {
      return false;
    }
    return "local".equals(conf.get("mapreduce.framework.name")) ||
      "local".equals(conf.get("mapred.job.tracker"));
  }

  @Override
  public String getJobLauncherRpcAddress(Configuration conf) {
    if (isMR2(conf)) {
      return conf.get("yarn.resourcemanager.address");
    } else {
      return conf.get("mapred.job.tracker");
    }
  }

  @Override
  public void setJobLauncherRpcAddress(Configuration conf, String val) {
    if (val.equals("local")) {
      // LocalClientProtocolProvider expects both parameters to be 'local'.
      if (isMR2(conf)) {
        conf.set("mapreduce.framework.name", val);
        conf.set("mapreduce.jobtracker.address", val);
      } else {
        conf.set("mapred.job.tracker", val);
      }
    }
    else {
      if (isMR2(conf)) {
        conf.set("yarn.resourcemanager.address", val);
      } else {
        conf.set("mapred.job.tracker", val);
      }
    }
  }

  public String getKerberosShortName(String kerberosLongName) throws IOException {
    KerberosName kerberosName = new KerberosName(kerberosLongName);
    return kerberosName.getShortName();
  }

  @Override
  public String getJobLauncherHttpAddress(Configuration conf) {
    if (isMR2(conf)) {
      return conf.get("yarn.resourcemanager.webapp.address");
    } else {
      return conf.get("mapred.job.tracker.http.address");
    }
  }

  @Override
  public String getMRFramework(Configuration conf) {
    return conf.get("mapreduce.framework.name");
  }

  @Override
  public void setMRFramework(Configuration conf, String framework) {
    conf.set("mapreduce.framework.name", framework);
  }

  protected boolean isExtendedAclEnabled(Configuration conf) {
    return Objects.equal(conf.get("dfs.namenode.acls.enabled"), "true");
  }

  @Override
  public long getDefaultBlockSize(FileSystem fs, Path path) {
    return fs.getDefaultBlockSize(path);
  }

  @Override
  public short getDefaultReplication(FileSystem fs, Path path) {
    return fs.getDefaultReplication(path);
  }

  @Override
  public boolean moveToAppropriateTrash(FileSystem fs, Path path, Configuration conf)
          throws IOException {
    return Trash.moveToAppropriateTrash(fs, path, conf);
  }

  @Override
  public void setTotalOrderPartitionFile(JobConf jobConf, Path partitionFile){
    try {
      Class<?> clazz = Class.forName("org.apache.hadoop.mapred.lib.TotalOrderPartitioner");
      try {
        java.lang.reflect.Method method = clazz.getMethod("setPartitionFile", Configuration.class, Path.class);
        method.invoke(null, jobConf, partitionFile);
      } catch(NoSuchMethodException nsme) {
        java.lang.reflect.Method method = clazz.getMethod("setPartitionFile", JobConf.class, Path.class);
        method.invoke(null, jobConf, partitionFile);
      }
    } catch(Exception e) {
      throw new IllegalStateException("Unable to find TotalOrderPartitioner.setPartitionFile", e);
    }
  }

  /**
   * Load the fair scheduler queue for given user if available
   */
  @Override
  public void refreshDefaultQueue(Configuration conf, String userName) throws IOException {
    String requestedQueue = YarnConfiguration.DEFAULT_QUEUE_NAME;
    if (isMR2(conf) && StringUtils.isNotBlank(userName) && isFairScheduler(conf)) {
      AllocationConfiguration allocConf = new AllocationConfiguration(conf);
      QueuePlacementPolicy queuePolicy = allocConf.getPlacementPolicy();
      if (queuePolicy != null) {
        requestedQueue = queuePolicy.assignAppToQueue(requestedQueue, userName);
        if (StringUtils.isNotBlank(requestedQueue)) {
          LOG.debug("Setting queue name to " + requestedQueue + " for user " + userName);
          conf.set(MR2_JOB_QUEUE_PROPERTY, requestedQueue);
        }
      }
    }
  }

  // verify if the configured scheduler is fair scheduler
  private boolean isFairScheduler (Configuration conf) {
    return FairScheduler.class.getName().
        equalsIgnoreCase(conf.get(YarnConfiguration.RM_SCHEDULER));
  }

  /**
   * Returns a shim to wrap MiniMrCluster
   */
  public MiniMrShim getMiniMrCluster(Configuration conf, int numberOfTaskTrackers,
                                     String nameNode, int numDir) throws IOException {
    return new MiniMrShim(conf, numberOfTaskTrackers, nameNode, numDir);
  }

  /**
   * Shim for MiniMrCluster
   */
  public class MiniMrShim implements HadoopShims.MiniMrShim {

    private final MiniMRCluster mr;
    private final Configuration conf;

    public MiniMrShim(Configuration conf, int numberOfTaskTrackers,
                      String nameNode, int numDir) throws IOException {
      this.conf = conf;

      JobConf jConf = new JobConf(conf);
      jConf.set("yarn.scheduler.capacity.root.queues", "default");
      jConf.set("yarn.scheduler.capacity.root.default.capacity", "100");

      mr = new MiniMRCluster(numberOfTaskTrackers, nameNode, numDir, null, null, jConf);
    }

    @Override
    public int getJobTrackerPort() throws UnsupportedOperationException {
      String address = conf.get("yarn.resourcemanager.address");
      address = StringUtils.substringAfterLast(address, ":");

      if (StringUtils.isBlank(address)) {
        throw new IllegalArgumentException("Invalid YARN resource manager port.");
      }

      return Integer.parseInt(address);
    }

    @Override
    public void shutdown() throws IOException {
      mr.shutdown();
    }

    @Override
    public void setupConfiguration(Configuration conf) {
      JobConf jConf = mr.createJobConf();
      for (Map.Entry<String, String> pair: jConf) {
        // TODO figure out why this was wrapped in
        // if(!"mapred.reduce.tasks".equalsIgnoreCase(pair.getKey()))
        conf.set(pair.getKey(), pair.getValue());
      }
    }

  }

  // Don't move this code to the parent class. There's a binary
  // incompatibility between hadoop 1 and 2 wrt MiniDFSCluster and we
  // need to have two different shim classes even though they are
  // exactly the same.
  public HadoopShims.MiniDFSShim getMiniDfs(Configuration conf,
      int numDataNodes,
      boolean format,
      String[] racks) throws IOException {
    return new MiniDFSShim(new MiniDFSCluster(conf, numDataNodes, format, racks));
  }

  /**
   * MiniDFSShim.
   *
   */
  public class MiniDFSShim implements HadoopShims.MiniDFSShim {
    private final MiniDFSCluster cluster;

    public MiniDFSShim(MiniDFSCluster cluster) {
      this.cluster = cluster;
    }

    public FileSystem getFileSystem() throws IOException {
      return cluster.getFileSystem();
    }

    public void shutdown() {
      cluster.shutdown();
    }
  }
  private volatile HCatHadoopShims hcatShimInstance;
  @Override
  public HCatHadoopShims getHCatShim() {
    if(hcatShimInstance == null) {
      hcatShimInstance = new HCatHadoopShims23();
    }
    return hcatShimInstance;
  }
  private final class HCatHadoopShims23 implements HCatHadoopShims {
    @Override
    public TaskID createTaskID() {
      return new TaskID("", 0, TaskType.MAP, 0);
    }

    @Override
    public TaskAttemptID createTaskAttemptID() {
      return new TaskAttemptID("", 0, TaskType.MAP, 0, 0);
    }

    @Override
    public org.apache.hadoop.mapreduce.TaskAttemptContext createTaskAttemptContext(Configuration conf,
                                                                                   org.apache.hadoop.mapreduce.TaskAttemptID taskId) {
      return new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(
              conf instanceof JobConf? new JobConf(conf) : conf,
              taskId);
    }

    @Override
    public org.apache.hadoop.mapred.TaskAttemptContext createTaskAttemptContext(org.apache.hadoop.mapred.JobConf conf,
                                                                                org.apache.hadoop.mapred.TaskAttemptID taskId, Progressable progressable) {
      org.apache.hadoop.mapred.TaskAttemptContext newContext = null;
      try {
        java.lang.reflect.Constructor construct = org.apache.hadoop.mapred.TaskAttemptContextImpl.class.getDeclaredConstructor(
                org.apache.hadoop.mapred.JobConf.class, org.apache.hadoop.mapred.TaskAttemptID.class,
                Reporter.class);
        construct.setAccessible(true);
        newContext = (org.apache.hadoop.mapred.TaskAttemptContext) construct.newInstance(
                new JobConf(conf), taskId, (Reporter) progressable);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return newContext;
    }

    @Override
    public JobContext createJobContext(Configuration conf,
                                       JobID jobId) {
      return new JobContextImpl(conf instanceof JobConf? new JobConf(conf) : conf,
              jobId);
    }

    @Override
    public org.apache.hadoop.mapred.JobContext createJobContext(org.apache.hadoop.mapred.JobConf conf,
                                                                org.apache.hadoop.mapreduce.JobID jobId, Progressable progressable) {
      try {
        java.lang.reflect.Constructor construct = org.apache.hadoop.mapred.JobContextImpl.class.getDeclaredConstructor(
          org.apache.hadoop.mapred.JobConf.class, org.apache.hadoop.mapreduce.JobID.class, Progressable.class);
        construct.setAccessible(true);
        return (org.apache.hadoop.mapred.JobContext) construct.newInstance(
                new JobConf(conf), jobId, progressable);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void commitJob(OutputFormat outputFormat, Job job) throws IOException {
      // Do nothing as this was fixed by MAPREDUCE-1447.
    }

    @Override
    public void abortJob(OutputFormat outputFormat, Job job) throws IOException {
      // Do nothing as this was fixed by MAPREDUCE-1447.
    }

    @Override
    public InetSocketAddress getResourceManagerAddress(Configuration conf) {
      String addr = conf.get("yarn.resourcemanager.address", "localhost:8032");

      return NetUtils.createSocketAddr(addr);
    }

    @Override
    public String getPropertyName(PropertyName name) {
      boolean mr2 = isMR2(new Configuration());
      switch (name) {
        case CACHE_ARCHIVES:
          if(mr2) {
            return "mapreduce.job.cache.archives";
          }
          return "mapred.cache.archives";
        case CACHE_FILES:
          if(mr2) {
            return "mapreduce.job.cache.files";
          }
          return "mapred.cache.files";
        case CACHE_SYMLINK:
          if(mr2) {
            return "mapreduce.job.cache.symlink.create";
          }
          return "mapred.create.symlink";
      }

      return "";
    }

    @Override
    public boolean isFileInHDFS(FileSystem fs, Path path) throws IOException {
      // In case of viewfs we need to lookup where the actual file is to know the filesystem in use.
      // resolvePath is a sure shot way of knowing which file system the file is.
      return "hdfs".equals(fs.resolvePath(path).toUri().getScheme());
    }
  }
  @Override
  public WebHCatJTShim getWebHCatShim(Configuration conf, UserGroupInformation ugi) throws IOException {
    return new WebHCatJTShim23(conf, ugi);//this has state, so can't be cached
  }

  private boolean isMR2(Configuration conf) {
    return "yarn".equalsIgnoreCase(conf.get("mapreduce.framework.name"));
  }

  @Override
  public HdfsFileStatus getFullFileStatus(Configuration conf, FileSystem fs,
      Path file) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(file);
    AclStatus aclStatus = null;
    if (isExtendedAclEnabled(conf)) {
      aclStatus = fs.getAclStatus(file);
    }
    return new Hadoop23FileStatus(fileStatus, aclStatus);
  }

  @Override
  public void setFullFileStatus(Configuration conf, HdfsFileStatus sourceStatus,
    FileSystem fs, Path target) throws IOException {
    String group = sourceStatus.getFileStatus().getGroup();
    //use FsShell to change group, permissions, and extended ACL's recursively
    try {
      FsShell fsShell = new FsShell();
      fsShell.setConf(conf);
      run(fsShell, new String[]{"-chgrp", "-R", group, target.toString()});

      if (isExtendedAclEnabled(conf)) {
        AclStatus aclStatus = ((Hadoop23FileStatus) sourceStatus).getAclStatus();
        List<AclEntry> aclEntries = aclStatus.getEntries();
        removeBaseAclEntries(aclEntries);

        //the ACL api's also expect the tradition user/group/other permission in the form of ACL
        FsPermission sourcePerm = sourceStatus.getFileStatus().getPermission();
        aclEntries.add(newAclEntry(AclEntryScope.ACCESS, AclEntryType.USER, sourcePerm.getUserAction()));
        aclEntries.add(newAclEntry(AclEntryScope.ACCESS, AclEntryType.GROUP, sourcePerm.getGroupAction()));
        aclEntries.add(newAclEntry(AclEntryScope.ACCESS, AclEntryType.OTHER, sourcePerm.getOtherAction()));

        //construct the -setfacl command
        String aclEntry = Joiner.on(",").join(aclStatus.getEntries());
        run(fsShell, new String[]{"-setfacl", "-R", "--set", aclEntry, target.toString()});
      } else {
        String permission = Integer.toString(sourceStatus.getFileStatus().getPermission().toShort(), 8);
        run(fsShell, new String[]{"-chmod", "-R", permission, target.toString()});
      }
    } catch (Exception e) {
      throw new IOException("Unable to set permissions of " + target, e);
    }
    try {
      if (LOG.isDebugEnabled()) {  //some trace logging
        getFullFileStatus(conf, fs, target).debugLog();
      }
    } catch (Exception e) {
      //ignore.
    }
  }

  public class Hadoop23FileStatus implements HdfsFileStatus {
    private FileStatus fileStatus;
    private AclStatus aclStatus;
    public Hadoop23FileStatus(FileStatus fileStatus, AclStatus aclStatus) {
      this.fileStatus = fileStatus;
      this.aclStatus = aclStatus;
    }
    @Override
    public FileStatus getFileStatus() {
      return fileStatus;
    }
    public AclStatus getAclStatus() {
      return aclStatus;
    }
    @Override
    public void debugLog() {
      if (fileStatus != null) {
        LOG.debug(fileStatus.toString());
      }
      if (aclStatus != null) {
        LOG.debug(aclStatus.toString());
      }
    }
  }

  /**
   * Create a new AclEntry with scope, type and permission (no name).
   *
   * @param scope
   *          AclEntryScope scope of the ACL entry
   * @param type
   *          AclEntryType ACL entry type
   * @param permission
   *          FsAction set of permissions in the ACL entry
   * @return AclEntry new AclEntry
   */
  private AclEntry newAclEntry(AclEntryScope scope, AclEntryType type,
      FsAction permission) {
    return new AclEntry.Builder().setScope(scope).setType(type)
        .setPermission(permission).build();
  }

  /**
   * Removes basic permission acls (unamed acls) from the list of acl entries
   * @param entries acl entries to remove from.
   */
  private void removeBaseAclEntries(List<AclEntry> entries) {
    Iterables.removeIf(entries, new Predicate<AclEntry>() {
      @Override
      public boolean apply(AclEntry input) {
          if (input.getName() == null) {
            return true;
          }
          return false;
      }
  });
  }

  class ProxyFileSystem23 extends ProxyFileSystem {
    public ProxyFileSystem23(FileSystem fs) {
      super(fs);
    }
    public ProxyFileSystem23(FileSystem fs, URI uri) {
      super(fs, uri);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f)
      throws FileNotFoundException, IOException {
      return new RemoteIterator<LocatedFileStatus>() {
        private RemoteIterator<LocatedFileStatus> stats =
            ProxyFileSystem23.super.listLocatedStatus(
                ProxyFileSystem23.super.swizzleParamPath(f));

        @Override
        public boolean hasNext() throws IOException {
          return stats.hasNext();
        }

        @Override
        public LocatedFileStatus next() throws IOException {
          LocatedFileStatus result = stats.next();
          return new LocatedFileStatus(
              ProxyFileSystem23.super.swizzleFileStatus(result, false),
              result.getBlockLocations());
        }
      };
    }
  }

  @Override
  public FileSystem createProxyFileSystem(FileSystem fs, URI uri) {
    return new ProxyFileSystem23(fs, uri);
  }
}
