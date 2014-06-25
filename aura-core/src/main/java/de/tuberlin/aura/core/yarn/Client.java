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
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* based on http://github.com/four2five/simple-yarn-app/ */

/**
 * requires:
 * - HDFS and YARN (ResourceManager and NodeManager) to be running
 *   (TODO: should be started automatically from script / AURA)
 * - jar of aura-core (e.g. aura-core-1.0-SNAPSHOT.jar built through mvn install)
 *   (TODO: jar(s) with the WorkloadManager and the TaskManager..)
 * 
 * run config (program arguments), e.g.:
 *   /bin/date 10 /home/lauritz/Code/Repositories/AURA/aura-core/target/aura-core-1.0-SNAPSHOT.jar
 */

public class Client {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    Configuration conf = new YarnConfiguration();


    // ---------------------------------------------------
    // Methods.
    // ---------------------------------------------------

    public static void main(String[] args) throws Exception {
        Client c = new Client();
        c.run(args);
    }

    public void run(String[] args) throws Exception {

        // read commandline arguments
        final String command = args[0];
        final int numberOfContainers = Integer.valueOf(args[1]);
        Path jarPath = new Path(args[2]);
        jarPath = FileSystem.get(conf).makeQualified(jarPath);

        // Create yarnClient
        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        // Create application
        YarnClientApplication app = yarnClient.createApplication();

        // Configure the container for the ApplicationMaster
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
        amContainer.setCommands(Collections.singletonList("$JAVA_HOME/bin/java" + " -Xmx256M" + " de.tuberlin.aura.core.yarn.ApplicationMaster" + " "
                + command + " " + String.valueOf(numberOfContainers) + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>"
                + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));

        // Setup jar for ApplicationMaster
        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        setupApplicationMasterJar(jarPath, appMasterJar);
        amContainer.setLocalResources(Collections.singletonMap("aura.jar", appMasterJar));

        // Setup CLASSPATH for ApplicationMaster
        Map<String, String> appMasterEnv = new HashMap<String, String>();
        setupApplicationMasterEnvironment(appMasterEnv);
        amContainer.setEnvironment(appMasterEnv);

        // Setup resource requirements for the ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(256);
        capability.setVirtualCores(1);

        // Setup ApplicationSubmissionContext for the application
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setApplicationName("AURA");
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue("default");

        // Submit application
        ApplicationId appId = appContext.getApplicationId();
        LOG.info("Submitting application " + appId);
        yarnClient.submitApplication(appContext);

        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED && appState != YarnApplicationState.KILLED && appState != YarnApplicationState.FAILED) {
            Thread.sleep(100);
            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
        }

        LOG.info("Application " + appId + " finished with" + " state " + appState + " at " + appReport.getFinishTime());

    }

    private void setupApplicationMasterJar(Path jarPath, LocalResource appMasterJar) throws IOException {
        FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
    }

    private void setupApplicationMasterEnvironment(Map<String, String> appMasterEnv) {
        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim());
        }
        Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), Environment.PWD.$() + File.separator + "*");
    }

}
