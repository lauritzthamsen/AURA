package de.tuberlin.aura.core.yarn;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

/* based on http://github.com/four2five/simple-yarn-app/ */

/**
 * requires:
 *  - HDFS and YARN (ResourceManager and NodeManager) to be running (TODO: should be started automatically from script / AURA)
 *  - jar of aura-core (e.g. aura-core-1.0-SNAPSHOT.jar built through mvn install) (TODO: jar(s) with the WorkloadManager and the TaskManager..)
 *
 * run config (program arguments), e.g.: /bin/date 10 /home/lauritz/Code/Repositories/AURA/aura-core/target/aura-core-1.0-SNAPSHOT.jar
 */

public class Client {

  Configuration conf = new YarnConfiguration();
  
  public void run(String[] args) throws Exception {
    final String command = args[0];
    final int numberOfContainers = Integer.valueOf(args[1]);
    Path jarPath = new Path(args[2]);
    jarPath = FileSystem.get(conf).makeQualified(jarPath);

    // Create yarnClient
    YarnConfiguration conf = new YarnConfiguration();
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    
    // Create application via yarnClient
    YarnClientApplication app = yarnClient.createApplication();

    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = 
        Records.newRecord(ContainerLaunchContext.class);
    amContainer.setCommands(
        Collections.singletonList(
            "$JAVA_HOME/bin/java" +
            " -Xmx256M" +
            " de.tuberlin.aura.core.yarn.ApplicationMaster" +
            " " + command +
            " " + String.valueOf(numberOfContainers) +
            " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + 
            " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr" 
            )
        );
    
    // Setup jar for ApplicationMaster
    LocalResource appMasterJar = Records.newRecord(LocalResource.class);
    setupAppMasterJar(jarPath, appMasterJar);
    amContainer.setLocalResources(
        Collections.singletonMap("simpleapp.jar", appMasterJar));

    // Setup CLASSPATH for ApplicationMaster
    Map<String, String> appMasterEnv = new HashMap<String, String>();
    setupAppMasterEnv(appMasterEnv);
    amContainer.setEnvironment(appMasterEnv);
    
    // Set up resource type requirements for ApplicationMaster
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(256);
    capability.setVirtualCores(1);

    // Finally, set-up ApplicationSubmissionContext for the application
    ApplicationSubmissionContext appContext = 
    app.getApplicationSubmissionContext();
    appContext.setApplicationName("simple-yarn-app"); // application name
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(capability);
    appContext.setQueue("default"); // queue 

    // Submit application
    ApplicationId appId = appContext.getApplicationId();
    System.out.println("Submitting application " + appId);
    yarnClient.submitApplication(appContext);
    
    ApplicationReport appReport = yarnClient.getApplicationReport(appId);
    YarnApplicationState appState = appReport.getYarnApplicationState();
    while (appState != YarnApplicationState.FINISHED && 
           appState != YarnApplicationState.KILLED && 
           appState != YarnApplicationState.FAILED) {
      Thread.sleep(100);
      appReport = yarnClient.getApplicationReport(appId);
      appState = appReport.getYarnApplicationState();
    }
    
    System.out.println(
        "Application " + appId + " finished with" +
    		" state " + appState + 
    		" at " + appReport.getFinishTime());

  }
  
  private void setupAppMasterJar(Path jarPath, LocalResource appMasterJar) throws IOException {
    FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
    appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
    appMasterJar.setSize(jarStat.getLen());
    appMasterJar.setTimestamp(jarStat.getModificationTime());
    appMasterJar.setType(LocalResourceType.FILE);
    appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
  }
  
  private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
    for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
          c.trim());
    }
    Apps.addToEnvironment(appMasterEnv,
        Environment.CLASSPATH.name(),
        Environment.PWD.$() + File.separator + "*");
  }
  
  public static void main(String[] args) throws Exception {
    Client c = new Client();
    c.run(args);
  }
}
