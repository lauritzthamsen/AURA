package de.tuberlin.aura.core.yarn;

import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* based on http://github.com/four2five/simple-yarn-app/ */

public class ApplicationMaster {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    // ---------------------------------------------------
    // Methods.
    // ---------------------------------------------------

    public static void main(String[] args) throws Exception {

        final String command = args[0];
        final int numberOfContainers = Integer.valueOf(args[1]);

        // Initialize clients to ResourceManager and NodeManagers
        Configuration conf = new YarnConfiguration();
        AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();
        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        // Register with ResourceManager
        LOG.info("registerApplicationMaster 0");
        rmClient.registerApplicationMaster("", 0, "");
        LOG.info("registerApplicationMaster 1");

        // Priority for worker containers (priorities are intra-application)
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        // Make container requests to ResourceManager
        for (int i = 0; i < numberOfContainers; ++i) {
            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
            LOG.info("Making res-req " + i);
            rmClient.addContainerRequest(containerAsk);
        }

        // Obtain allocated containers and launch
        int allocatedContainers = 0;
        // We need to start counting completed containers while still allocating them since initial ones may complete
        // while we're allocating subsequent containers and this ApplicationMaster will hang indefinitely, if we miss
        // those notifications.
        int completedContainers = 0;
        while (allocatedContainers < numberOfContainers) {
            AllocateResponse response = rmClient.allocate(0);
            for (Container container : response.getAllocatedContainers()) {
                ++allocatedContainers;

                // Launch container with ContainerLaunchContext
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(Collections.singletonList(command + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>"
                        + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));
                LOG.info("Launching container " + allocatedContainers);
                nmClient.startContainer(container, ctx);
            }
            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                ++completedContainers;
                LOG.info("Completed container " + completedContainers);
            }
            Thread.sleep(100);
        }

        // Wait for the containers to complete
        while (completedContainers < numberOfContainers) {
            AllocateResponse response = rmClient.allocate(completedContainers / numberOfContainers);
            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                ++completedContainers;
                LOG.info("Completed container " + completedContainers);
            }
            Thread.sleep(100);
        }

        // Unregister with the ResourceManager
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
    }
}
