/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.util.ProcessIdFileReader;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import java.net.InetSocketAddress;


/**
 * This executor will launch a docker container and run the task inside the container.
 */
public class SecureDockerContainerExecutor extends ContainerExecutor {

  private static final Log LOG = LogFactory
      .getLog(SecureDockerContainerExecutor.class);
  public static final String DOCKER_CONTAINER_EXECUTOR_SCRIPT = "docker_container_executor";

  // This validates that the image is a proper docker image and would not crash docker.
  public static final String DOCKER_IMAGE_PATTERN = "^(([\\w\\.-]+)(:\\d+)*\\/)?[\\w\\.:-]+$";


  private final Pattern dockerImagePattern;

  private String containerExecutorExe;

  private String dockeContainerExecConfFilePath;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    String yarnHomeEnvVar =
        System.getenv(ApplicationConstants.Environment.HADOOP_YARN_HOME.key());
    File hadoopBin = new File(yarnHomeEnvVar, "bin");
    String execPath = conf.get(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_FILE_PATH);
    if (execPath != null) {
      hadoopBin = new File(execPath);
    }
    containerExecutorExe =
        new File(hadoopBin, "docker_container_executor").getAbsolutePath();
  }

  public SecureDockerContainerExecutor() {
    this.dockerImagePattern = Pattern.compile(DOCKER_IMAGE_PATTERN);
  }

  @Override
  public void init() throws IOException {
    String auth = getConf().get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION);
    if (auth != null && !auth.equals("simple")) {
      throw new IllegalStateException("SecureDockerContainerExecutor only works with simple authentication mode");
    }
    dockeContainerExecConfFilePath = getConf().get(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_CONF_FILE_PATH,
        YarnConfiguration.NM_DEFAULT_DOCKER_CONTAINER_EXECUTOR_CONF_FILE_PATH);
    if (!new File(dockeContainerExecConfFilePath).exists()) {
      throw new IllegalStateException("Invalid docker exec config file path: " + dockeContainerExecConfFilePath);
    }
  }

  @Override
  public void renameLocalDirForCleanUp(FileContext lfs, String localDir, String localSubDir, long currentTimeStamp) {
    // 需要将目录的所有者切换成默认的（sankuai），方便回收空间
    List<String> command = new ArrayList<String>();
    String source = new File(localDir, localSubDir).getAbsolutePath();
    String dest = new File(localDir, localSubDir + "_DEL_" + currentTimeStamp).getAbsolutePath();

    command.addAll(Arrays.asList(containerExecutorExe,
        "--type", Integer.toString(Commands.RENAME_LOCAL_FOR_CLEANUP.getValue()),
        "--source-path", source,
        "--dest-path", dest,
        "--conf", dockeContainerExecConfFilePath));

    String[] commanddArray = command.toArray(new String[command.size()]);
    ShellCommandExecutor shExec = new ShellCommandExecutor(commanddArray);

    if (LOG.isDebugEnabled())
      LOG.info("Set DEL File as default: " + Arrays.toString(commanddArray));
    try {
      shExec.execute();
      if (LOG.isDebugEnabled()) {
        logOutput(shExec.getOutput());
      }
    } catch (Shell.ExitCodeException e) {
      int exitCode = shExec.getExitCode();
      LOG.warn("Exit code from container is : " + exitCode, e);
      logOutput(shExec.getOutput());
    } catch (IOException e) {
      LOG.info(e.getMessage());
      logOutput(shExec.getOutput());
    } finally {
      if (shExec != null) {
        shExec.close();
      }
    }
  }

  @VisibleForTesting
  public void buildMainArgs(List<String> command, String user, String appId,
                            String locId, InetSocketAddress nmAddr, List<String> localDirs) {
    ContainerLocalizer.buildMainArgs(command, user, appId, locId, nmAddr,
        localDirs);
  }

  @Override
  public synchronized void startLocalizer(Container container, Path nmPrivateContainerTokensPath,
                                          InetSocketAddress nmAddr, String user, String appId, String locId,
                                          LocalDirsHandlerService dirsHandler)
      throws IOException, InterruptedException {
    String containerImageName = null;
    if (container != null) {
      Map<String, String> envs = container.getLaunchContext().getEnvironment();
      containerImageName = envs.get(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME);
      LOG.info("startLocalizer use image :" + containerImageName);
    }
    List<String> localDirs = dirsHandler.getLocalDirs();
    List<String> logDirs = dirsHandler.getLogDirs();

    String appWorkDir = getWorkingDir(localDirs).toString() + File.separator + ContainerLocalizer.USERCACHE +
        File.separator + user + File.separator + ContainerLocalizer.APPCACHE + File.separator + appId;

    List<String> command = new ArrayList<String>();
    command.addAll(Arrays.asList(containerExecutorExe,
        "--type", Integer.toString(Commands.INITIALIZE_CONTAINER.getValue()),
        "--user", user,
        "--app-id", appId,
        "--local-dirs", StringUtils.join(",", localDirs),
        "--log-dirs", StringUtils.join(",", logDirs),
        "--work-dir", appWorkDir,
        "--loc-id", locId,
        "--conf", dockeContainerExecConfFilePath,
        "--token-path", nmPrivateContainerTokensPath.toString()));

    if (containerImageName != null) {
      command.add("--image-name");
      command.add(containerImageName);
    }

    Set<String> volumes = new HashSet<String>();
    String javaHome = System.getProperty("java.home");
    volumes.add(javaHome);
    if (System.getenv().containsKey(ApplicationConstants.Environment.JAVA_HOME.name()))
      javaHome = System.getenv(ApplicationConstants.Environment.JAVA_HOME.name());
    String hadoopHome = "/opt/meituan/hadoop";

    if (System.getenv().containsKey("HADOOP_HOME"))
      hadoopHome = System.getenv().get("HADOOP_HOME");
    String confDir = hadoopHome + "/etc/hadoop";
    if (System.getenv().containsKey("YARN_CONF_DIR"))
      confDir = System.getenv().get("YARN_CONF_DIR");
    String yarnLogDir = hadoopHome + "/logs";
    if (System.getenv().containsKey("YARN_LOG_DIR"))
      yarnLogDir = System.getenv().get("YARN_LOG_DIR");

    String hadoopLogDir = hadoopHome + "/logs";
    if (System.getenv().containsKey("HADOOP_LOG_DIR"))
      hadoopLogDir = System.getenv().get("HADOOP_LOG_DIR");

    volumes.add(hadoopHome);

    if (System.getenv().containsKey(ApplicationConstants.Environment.HADOOP_YARN_HOME.name())) {
      volumes.add(System.getenv(ApplicationConstants.Environment.HADOOP_YARN_HOME.name()));
    }
    if (System.getenv().containsKey(ApplicationConstants.Environment.HADOOP_HDFS_HOME.name())) {
      volumes.add(System.getenv(ApplicationConstants.Environment.HADOOP_HDFS_HOME.name()));
    }
    if (System.getenv().containsKey(ApplicationConstants.Environment.HADOOP_COMMON_HOME.name())) {
      volumes.add(System.getenv(ApplicationConstants.Environment.HADOOP_COMMON_HOME.name()));
    }
    if (System.getenv().containsKey(ApplicationConstants.Environment.HADOOP_CONF_DIR.name())) {
      volumes.add(System.getenv(ApplicationConstants.Environment.HADOOP_CONF_DIR.name()));
    }

    volumes.add(javaHome);
    volumes.addAll(localDirs);
    volumes.addAll(logDirs);
    volumes.add(confDir);
    volumes.add(yarnLogDir);
    volumes.add(hadoopLogDir);

    List<String> dockerLocalizerCmd = new ArrayList<String>();
    File jvm =                                  // use same jvm as parent
        new File(new File(javaHome, "bin"), "java");
    dockerLocalizerCmd.add(jvm.toString());
    dockerLocalizerCmd.add("-classpath");
    dockerLocalizerCmd.add(System.getProperty("java.class.path"));
    String javaLibPath = System.getProperty("java.library.path");
    if (javaLibPath != null) {
      dockerLocalizerCmd.add("-Djava.library.path=" + javaLibPath);
    }
    buildMainArgs(dockerLocalizerCmd, user, appId, locId, nmAddr, localDirs);

    command.add("--cmd");
    command.add(Joiner.on(",").join(dockerLocalizerCmd));
    command.add("--volumes");
    command.add(Joiner.on(",").join(volumes));
    String[] commandArray = command.toArray(new String[command.size()]);

    ShellCommandExecutor shExec = new ShellCommandExecutor(commandArray);

    if (LOG.isDebugEnabled()) {
      LOG.debug("initApplication: " + Arrays.toString(commandArray));
    }
    try {
      shExec.execute();
      if (LOG.isDebugEnabled()) {
        logOutput(shExec.getOutput());
      }
    } catch (Shell.ExitCodeException e) {
      int exitCode = shExec.getExitCode();
      LOG.warn("Exit code from container " + locId + " startLocalizer is : "
          + exitCode, e);
      logOutput(shExec.getOutput());
      throw new IOException("Application " + appId + " initialization failed" +
          " (exitCode=" + exitCode + ") with output: " + shExec.getOutput(), e);
    } finally {
      if (shExec != null) {
        shExec.close();
      }
    }
  }


  @Override
  public int launchContainer(Container container,
                             Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath,
                             String userName, String appId, Path containerWorkDir,
                             List<String> localDirs, List<String> logDirs) throws IOException {
    Map<String, String> envs = container.getLaunchContext().getEnvironment();
    String containerImageName = envs.get(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME);

    if (LOG.isDebugEnabled()) {
      LOG.debug("containerImageName from launchContext: " + containerImageName);
    }
    Preconditions.checkArgument(!Strings.isNullOrEmpty(containerImageName), "Container image must not be null");
//    containerImageName = containerImageName.replaceAll("['\"]", "");
//    Preconditions.checkArgument(saneDockerImage(containerImageName), "Image: " + containerImageName + " is not a proper docker image");

    ContainerId containerId = container.getContainerId();

    Set<String> volumes = new HashSet<String>();
    String javaHome = System.getProperty("java.home");
    volumes.add(javaHome);
    if (envs.containsKey(ApplicationConstants.Environment.JAVA_HOME.name()))
      javaHome = envs.get(ApplicationConstants.Environment.JAVA_HOME.name());
    String hadoopHome = "/opt/meituan/hadoop";

    if (envs.containsKey("HADOOP_HOME"))
      hadoopHome = envs.get("HADOOP_HOME");
    String confDir = hadoopHome + "/etc/hadoop";
    if (envs.containsKey("YARN_CONF_DIR"))
      confDir = envs.get("YARN_CONF_DIR");
    String yarnLogDir = hadoopHome + "/logs";
    if (envs.containsKey("YARN_LOG_DIR"))
      yarnLogDir = envs.get("YARN_LOG_DIR");

    String hadoopLogDir = hadoopHome + "/logs";
    if (envs.containsKey("HADOOP_LOG_DIR"))
      hadoopLogDir = envs.get("HADOOP_LOG_DIR");

    volumes.add(hadoopHome);

    if (envs.containsKey(ApplicationConstants.Environment.HADOOP_YARN_HOME.name())) {
      volumes.add(envs.get(ApplicationConstants.Environment.HADOOP_YARN_HOME.name()));
    }
    if (envs.containsKey(ApplicationConstants.Environment.HADOOP_HDFS_HOME)) {
      volumes.add(envs.get(ApplicationConstants.Environment.HADOOP_HDFS_HOME.name()));
    }
    if (envs.containsKey(ApplicationConstants.Environment.HADOOP_COMMON_HOME.name())) {
      volumes.add(envs.get(ApplicationConstants.Environment.HADOOP_COMMON_HOME.name()));
    }
    if (envs.containsKey(ApplicationConstants.Environment.HADOOP_CONF_DIR.name())) {
      volumes.add(envs.get(ApplicationConstants.Environment.HADOOP_CONF_DIR.name()));
    }

    volumes.add(javaHome);
    volumes.addAll(localDirs);
    volumes.addAll(logDirs);
    volumes.add(confDir);
    volumes.add(yarnLogDir);
    volumes.add(hadoopLogDir);

    String containerIdStr = ConverterUtils.toString(containerId);

    Path pidFile = getPidFilePath(containerId);
    if (pidFile == null) {
      LOG.warn("Container " + containerIdStr
          + " was marked as inactive. Returning terminated error");
      return ExitCode.TERMINATED.getExitCode();
    } else {
      LOG.info("pidFile is " + pidFile.toUri().toString());
    }

    int cpu = container.getResource().getVirtualCores();
    int memory = container.getResource().getMemory();
    LOG.debug("cpu:  " + cpu + " memory:" + memory);

    List<String> preCommand = new ArrayList<String>();
    preCommand.addAll(Arrays.asList(containerExecutorExe,
        "--cpu", String.valueOf(cpu),
        "--memory", String.valueOf(memory),
        "--type", Integer.toString(Commands.LAUNCH_CONTAINER.getValue()),
        "--user", userName,
        "--app-id", appId,
        "--container-id", containerIdStr,
        "--local-dirs", StringUtils.join(",", localDirs),
        "--log-dirs", StringUtils.join(",", logDirs),
        "--work-dir", containerWorkDir.toString(),
        "--script-path", nmPrivateContainerScriptPath.toString(),
        "--token-path", nmPrivateTokensPath.toString(),
        "--image-name", containerImageName,
        "--conf", dockeContainerExecConfFilePath,
        "--volumes", Joiner.on(",").join(volumes),
        "--pid-path", pidFile.toString()));

    if (envs.containsKey("CUDA_VISIBLE_DEVICES") && envs.get("CUDA_VISIBLE_DEVICES").length() > 0) {
      preCommand.add("--gpus");
      preCommand.add(envs.get("CUDA_VISIBLE_DEVICES"));
    }

    String[] preCommanddArray = preCommand.toArray(new String[preCommand.size()]);
    ShellCommandExecutor preShExec = new ShellCommandExecutor(preCommanddArray);
    if (LOG.isDebugEnabled()) {
      LOG.debug("pre launch container: " + Arrays.toString(preCommanddArray));
    }
    try {
      preShExec.execute();
      if (LOG.isDebugEnabled()) {
        logOutput(preShExec.getOutput());
      }
    } catch (Shell.ExitCodeException e) {
      int exitCode = preShExec.getExitCode();
      LOG.warn("Exit code from container " + containerIdStr + " preLaunchContainer is : "
          + exitCode, e);
      logOutput(preShExec.getOutput());
      throw new IOException("Application " + appId + " preLaunchContainer failed" +
          " (exitCode=" + exitCode + ") with output: " + preShExec.getOutput(), e);
    } finally {
      if (preShExec != null) {
        preShExec.close();
      }
    }

    ShellCommandExecutor shExec = null;
    try {
      // Setup command to run
      Path script = new Path(containerWorkDir, DOCKER_CONTAINER_EXECUTOR_SCRIPT + ".sh");
      String[] command = new String[]{"bash", script.toUri().toString()};

      if (LOG.isDebugEnabled()) {
        LOG.debug("launchContainer: " + Joiner.on(" ").join(command));
      }

      shExec = new ShellCommandExecutor(
          command,
          new File(containerWorkDir.toUri().getPath()),
          container.getLaunchContext().getEnvironment());      // sanitized env
      if (isContainerActive(containerId)) {
        shExec.execute();
      } else {
        LOG.info("Container " + containerIdStr +
            " was marked as inactive. Returning terminated error");
        return ExitCode.TERMINATED.getExitCode();
      }
    } catch (IOException e) {
      if (null == shExec) {
        return -1;
      }

      int exitCode = shExec.getExitCode();
      LOG.warn("Exit code from container " + containerId + " is : " + exitCode);
      // 143 (SIGTERM) and 137 (SIGKILL) exit codes means the container was
      // terminated/killed forcefully. In all other cases, log the
      // container-executor's output
      if (exitCode != ExitCode.FORCE_KILLED.getExitCode()
          && exitCode != ExitCode.TERMINATED.getExitCode()) {
        LOG.warn("Exception from container-launch with container ID: "
            + containerId + " and exit code: " + exitCode, e);
        logOutput(shExec.getOutput());
        String diagnostics = "Exception from container-launch: \n"
            + StringUtils.stringifyException(e) + "\n" + shExec.getOutput();
        container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
            diagnostics));
      } else {
        container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
            "Container killed on request. Exit code is " + exitCode));
      }
      return exitCode;
    } finally {
      if (shExec != null) {
        shExec.close();
      }
    }
    return 0;
  }

  @Override
  public void writeLaunchEnv(OutputStream out, Map<String, String> environment, Map<Path, List<String>> resources, List<String> command, String containerWorkDir) throws IOException {
    ContainerLaunch.ShellScriptBuilder sb = ContainerLaunch.ShellScriptBuilder.create();
    LOG.info("change containerWorkDir : " + containerWorkDir);

    sb.chdir(containerWorkDir);
    if (environment.containsKey(ApplicationConstants.Environment.HADOOP_YARN_HOME.name())) {
      sb.env(ApplicationConstants.Environment.HADOOP_YARN_HOME.name(), environment.get(ApplicationConstants.Environment.HADOOP_YARN_HOME.name()).toString());
    }
    if (environment.containsKey(ApplicationConstants.Environment.HADOOP_COMMON_HOME.name())) {
      sb.env(ApplicationConstants.Environment.HADOOP_COMMON_HOME.name(), environment.get(ApplicationConstants.Environment.HADOOP_COMMON_HOME.name()).toString());
    }
    if (environment.containsKey(ApplicationConstants.Environment.HADOOP_CONF_DIR.name())) {
      sb.env(ApplicationConstants.Environment.HADOOP_CONF_DIR.name(), environment.get(ApplicationConstants.Environment.HADOOP_CONF_DIR.name()).toString());
    }
    if (environment.containsKey(ApplicationConstants.Environment.HADOOP_HDFS_HOME.name())) {
      sb.env("HADOOP_HOME", environment.get(ApplicationConstants.Environment.HADOOP_HDFS_HOME.name()).toString());
      sb.env(ApplicationConstants.Environment.HADOOP_HDFS_HOME.name(), environment.get(ApplicationConstants.Environment.HADOOP_HDFS_HOME.name()).toString());
    }
    if (environment.containsKey(ApplicationConstants.Environment.JAVA_HOME.name())) {
      sb.env(ApplicationConstants.Environment.JAVA_HOME.name(), environment.get(ApplicationConstants.Environment.JAVA_HOME.name()).toString());
    }
    //TODO  SecureDockerContainerExecutor TF 访问HDFS需要执行下面两条命令，加入Kerberos 时候还需要其他命令，https://www.tensorflow.org/deploy/hadoop，未来使用其他方式去代码，通过配置，暂时使用代码
    sb.script("TMPCLASSPATH=$CLASSPATH");
    sb.script("source ${HADOOP_HDFS_HOME}/libexec/hadoop-config.sh");
    sb.script("export CLASSPATH=$(${HADOOP_HDFS_HOME}/bin/hadoop classpath --glob)");

    String cuda = "";
    if (environment.containsKey("CUDA_VISIBLE_DEVICES") && environment.get("CUDA_VISIBLE_DEVICES").length() > 0) {
      String[] gpus = environment.get("CUDA_VISIBLE_DEVICES").split(",");
      int index = 0;
      for (String gpu : gpus) {
        if (!Strings.isNullOrEmpty(gpu)) {
          cuda += (index + ",");
          index++;
        }
      }
      if (cuda.endsWith(","))
        cuda = cuda.substring(0, cuda.length() - 1);
      sb.env("GPU_INFO", cuda);
      sb.env("CUDA_VISIBLE_DEVICES", cuda);
    }

    //TODO 需要讨论哪些环境变量暴露给用户，哪些需要禁止用户修改、哪些是替换、哪些是自动追加，需要做一个统一
    Set<String> exclusionSet = new HashSet<String>();
    exclusionSet.add(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME);
    exclusionSet.add(ApplicationConstants.Environment.HADOOP_YARN_HOME.name());
    exclusionSet.add(ApplicationConstants.Environment.HADOOP_COMMON_HOME.name());
    exclusionSet.add(ApplicationConstants.Environment.HADOOP_HDFS_HOME.name());
    exclusionSet.add(ApplicationConstants.Environment.HADOOP_CONF_DIR.name());
    exclusionSet.add(ApplicationConstants.Environment.JAVA_HOME.name());
    exclusionSet.add("GPU_INFO");
    exclusionSet.add("CUDA_VISIBLE_DEVICES");

    if (environment != null) {
      for (Map.Entry<String, String> env : environment.entrySet()) {
        if (!exclusionSet.contains(env.getKey())) {
          //TODO  Tensorflow 访问HDFS失败，找不到对应的jar包，可能Docker和直接机器有区别，暂时直接将前面设置的Hadoop相关jar包设置在CLASSPATH。需要查找原因,
          if ("CLASSPATH".equals(env.getKey().toString())) {
            sb.env(env.getKey().toString(), env.getValue().toString() + ":$CLASSPATH:$TMPCLASSPATH");
          } else if ("LD_LIBRARY_PATH".equals(env.getKey().toString())) {
            sb.env(env.getKey().toString(), "$LD_LIBRARY_PATH:" + env.getValue().toString());
          } else {
            sb.env(env.getKey().toString(), env.getValue().toString());
          }
        }
      }
    }
    if (resources != null) {
      for (Map.Entry<Path, List<String>> entry : resources.entrySet()) {
        for (String linkName : entry.getValue()) {
          sb.symlink(entry.getKey(), new Path(linkName));
        }
      }
    }

    sb.command(command);

    PrintStream pout = null;
    PrintStream ps = null;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      pout = new PrintStream(out, false, "UTF-8");
      if (LOG.isDebugEnabled()) {
        ps = new PrintStream(baos, false, "UTF-8");
        sb.write(ps);
      }
      sb.write(pout);

    } finally {
      if (out != null) {
        out.close();
      }
      if (ps != null) {
        ps.close();
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Script: " + baos.toString("UTF-8"));
    }
  }

  @Override
  public boolean signalContainer(String user, String pid, Signal signal)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending signal " + signal.getValue() + " to pid " + pid
          + " as user " + user);
    }
    List<String> command = new ArrayList<String>();
    command.addAll(Arrays.asList(containerExecutorExe,
        "--type", Integer.toString(Commands.SIGNAL_CONTAINER.getValue()),
        "--conf", dockeContainerExecConfFilePath,
        "--user", user,
        "--pid", pid,
        "--signal", Integer.toString(signal.getValue())
    ));

    String[] commandArray = command.toArray(new String[command.size()]);
    ShellCommandExecutor shExec = new ShellCommandExecutor(commandArray);
    if (LOG.isDebugEnabled()) {
      LOG.debug("signal Container: " + Arrays.toString(commandArray));
    }
    try {
      shExec.execute();
      if (LOG.isDebugEnabled())
        logOutput(shExec.getOutput());
    } catch (Shell.ExitCodeException e) {
      // docker container 会很干净的退出运行环境，不用手动的去kill 进程，如果有异常，查看进程存不存在
      if(signal == Signal.KILL || signal == Signal.TERM || signal == Signal.QUIT){
        boolean isExist = signalContainer(user, pid, Signal.NULL);
        if(!isExist){
          LOG.info(pid + " exit succeed.");
          return true;
        }
      }
      int exitCode = shExec.getExitCode();
      LOG.warn("Exit code from signal " + signal +" to " + pid  +  " container is : " + exitCode);
      logOutput(shExec.getOutput());
      return false;
    } finally {
      if (shExec != null) {
        shExec.close();
      }
    }

    return true;
  }

  @Override
  public boolean isContainerProcessAlive(String user, String pid)
      throws IOException {
    return signalContainer(user, pid, Signal.NULL);
  }

  @Override
  public int reacquireContainer(String user, ContainerId containerId)
      throws IOException, InterruptedException {

    Path pidPath = getPidFilePath(containerId);
    if (pidPath == null) {
      LOG.warn(containerId + " is not active, returning terminated error");
      return ExitCode.TERMINATED.getExitCode();
    }

    String pid = ProcessIdFileReader.getProcessId(pidPath);
    if (pid == null) {
      throw new IOException("Unable to determine pid for " + containerId);
    }

    LOG.info("Reacquiring " + containerId + " with pid " + pid);
    List<String> command = new ArrayList<String>();
    command.addAll(Arrays.asList(containerExecutorExe,
        "--conf", dockeContainerExecConfFilePath,
        "--type", Integer.toString(Commands.WAIT_CONTAINER.getValue()),
        "--user", user,
        "--container-id", containerId.toString()
        ));
    String[] commanddArray = command.toArray(new String[command.size()]);
    ShellCommandExecutor shExec = new ShellCommandExecutor(commanddArray);
    if (LOG.isDebugEnabled()) {
      LOG.debug("wait container exit: " + Arrays.toString(commanddArray));
    }
    int exitCode = 0;
    try {
      shExec.execute();
      if (LOG.isDebugEnabled()) {
        logOutput(shExec.getOutput());
      }
    } catch (Shell.ExitCodeException e) {
      exitCode = shExec.getExitCode();
      LOG.warn("wait exit container: " + containerId+ " exitcode:" + exitCode);
      logOutput(shExec.getOutput());
    } finally {
      if (shExec != null) {
        shExec.close();
      }
    }

    LOG.info("wait " + containerId + " exit code: " + exitCode);
    if(exitCode == 0){
      return exitCode;
    }

    while (isContainerProcessAlive(user, pid)) {
      Thread.sleep(1000);
    }

    // wait for exit code file to appear
    String exitCodeFile = ContainerLaunch.getExitCodeFile(pidPath.toString());
    File file = new File(exitCodeFile);
    final int sleepMsec = 500;
    int msecLeft = 5000;
    int retTimes = 0;
    while (!file.exists() && msecLeft >= 0) {
      if (!isContainerActive(containerId)) {
        if(retTimes > 5 ) {
          LOG.info(containerId + " was deactivated");
          return ExitCode.TERMINATED.getExitCode();
        }
        retTimes ++ ;
      }

      Thread.sleep(sleepMsec);

      msecLeft -= sleepMsec;
    }
    if (msecLeft < 0) {
      throw new IOException("Timeout while waiting for exit code from "
          + containerId);
    }

    try {
      return Integer.parseInt(FileUtils.readFileToString(file).trim());
    } catch (NumberFormatException e) {
      throw new IOException("Error parsing exit code from pid " + pid, e);
    }
  }


  @Override
  public void deleteAsUser(String user, Path subDir, Path... baseDirs)
      throws IOException, InterruptedException {
    List<Path> delPaths = new ArrayList<Path>();
    if (baseDirs == null || baseDirs.length == 0) {
      LOG.info("Deleting absolute path : " + subDir);
      //Maybe retry
      LOG.warn("delete returned false for path: [" + subDir + "]");
      delPaths.add(subDir);
    } else {
      for (Path baseDir : baseDirs) {
        Path del = subDir == null ? baseDir : new Path(baseDir, subDir);
        LOG.info("Deleting path : " + del);
        delPaths.add(del);
      }
    }

    List<String> command = new ArrayList<String>();
    command.addAll(Arrays.asList(containerExecutorExe,
        "--conf", dockeContainerExecConfFilePath,
        "--type", Integer.toString(Commands.DELETE_AS_USER.getValue()),
        "--del-dirs", StringUtils.join(",", delPaths)));

    String[] commandArray = command.toArray(new String[command.size()]);
    ShellCommandExecutor shExec = new ShellCommandExecutor(commandArray);
    if (LOG.isDebugEnabled()) {
      LOG.debug("initApplication: " + Arrays.toString(commandArray));
    }
    try {
      shExec.execute();
      if (LOG.isDebugEnabled())
        logOutput(shExec.getOutput());
    } catch (Shell.ExitCodeException e) {
      int exitCode = shExec.getExitCode();
      LOG.warn("Exit code from container  deleteAsUser is : " + exitCode, e);
      logOutput(shExec.getOutput());
    } finally {
      if (shExec != null) {
        shExec.close();
      }
    }
  }


  @Override
  public void cleanUpImagesAndTmpDir(){
    List<String> command = new ArrayList<String>();
    command.addAll(Arrays.asList(containerExecutorExe,
        "--conf", dockeContainerExecConfFilePath,
        "--type", Integer.toString(Commands.CLEAN_IMAGE_AND_TMP.getValue())));
    String[] commanddArray = command.toArray(new String[command.size()]);
    ShellCommandExecutor shExec = new ShellCommandExecutor(commanddArray);
    if (LOG.isDebugEnabled()) {
      LOG.debug("clean up images and tmp dir: " + Arrays.toString(commanddArray));
    }
    int exitCode = -1;
    try {
      shExec.execute();
      logOutput(shExec.getOutput());
    } catch (Shell.ExitCodeException e) {
      exitCode = shExec.getExitCode();
      LOG.warn("clean image and tmp dir failed." + exitCode, e);
      logOutput(shExec.getOutput());
    } catch (IOException e) {
      LOG.warn("clean image failed.", e);
    } finally {
      if (shExec != null) {
        shExec.close();
      }
    }
  }

  private long getDiskFreeSpace(Path base) throws IOException {
    return FileContext.getLocalFSFileContext().getFsStatus(base).getRemaining();
  }


  protected Path getWorkingDir(List<String> localDirs) throws IOException {
    Path appStorageDir = null;
    long totalAvailable = 0L;
    long[] availableOnDisk = new long[localDirs.size()];
    int i = 0;
    // randomly choose the app directory
    // the chance of picking a directory is proportional to
    // the available space on the directory.
    // firstly calculate the sum of all available space on these directories
    for (String localDir : localDirs) {
      Path curBase = new Path(localDir);
      long space = 0L;
      try {
        space = getDiskFreeSpace(curBase);
      } catch (IOException e) {
        LOG.warn("Unable to get Free Space for " + curBase.toString(), e);
      }
      availableOnDisk[i++] = space;
      totalAvailable += space;
    }

    // throw an IOException if totalAvailable is 0.
    if (totalAvailable <= 0L) {
      throw new IOException("Not able to find a working directory for ");
    }

    // make probability to pick a directory proportional to
    // the available space on the directory.
    long randomPosition = RandomUtils.nextLong() % totalAvailable;
    int dir = 0;
    // skip zero available space directory,
    // because totalAvailable is greater than 0 and randomPosition
    // is less than totalAvailable, we can find a valid directory
    // with nonzero available space.
    while (availableOnDisk[dir] == 0L) {
      dir++;
    }
    while (randomPosition > availableOnDisk[dir]) {
      randomPosition -= availableOnDisk[dir++];
    }
    appStorageDir = new Path(localDirs.get(dir));
    return appStorageDir;
  }


  /**
   * List of commands that the setuid script will execute.
   */
  enum Commands {
    INITIALIZE_CONTAINER(0),
    LAUNCH_CONTAINER(1),
    SIGNAL_CONTAINER(2),
    DELETE_AS_USER(3),
    RENAME_LOCAL_FOR_CLEANUP(4),
    WAIT_CONTAINER(5),
    CLEAN_IMAGE_AND_TMP(6);

    private int value;

    Commands(int value) {
      this.value = value;
    }

    int getValue() {
      return value;
    }
  }
}