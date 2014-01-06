package de.tuberlin.aura.workloadmanager;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.iosystem.IOEvents.ControlEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.TaskStateEvent;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.ClientWMProtocol;

public class WorkloadManager implements ClientWMProtocol {

	// ---------------------------------------------------
	// Inner Classes.
	// ---------------------------------------------------

	private final class IORedispatcher extends EventHandler {

		@Handle(event = TaskStateEvent.class)
		private void handleTaskReadyEvent(final TaskStateEvent event) {
			registeredToplogies.get(event.topologyID).dispatchEvent(event);
		}
	}

	// ---------------------------------------------------
	// Constructors.
	// ---------------------------------------------------

	public WorkloadManager(final MachineDescriptor machine) {
		// sanity check.
		if (machine == null)
			throw new IllegalArgumentException("machine == null");

		this.machine = machine;

		this.ioManager = new IOManager(this.machine);

		this.rpcManager = new RPCManager(ioManager);

		this.infrastructureManager = new InfrastructureManager();

		this.registeredToplogies = new ConcurrentHashMap<UUID, TopologyController>();

		rpcManager.registerRPCProtocolImpl(this, ClientWMProtocol.class);

		this.ioHandler = new IORedispatcher();

		final String[] IOEvents = { ControlEventType.CONTROL_EVENT_TASK_STATE };

		ioManager.addEventListener(IOEvents, ioHandler);
	}

	// ---------------------------------------------------
	// Fields.
	// ---------------------------------------------------

	private static final Logger LOG = Logger.getLogger(WorkloadManager.class);

	private final MachineDescriptor machine;

	private final IOManager ioManager;

	private final RPCManager rpcManager;

	private final InfrastructureManager infrastructureManager;

	private final Map<UUID, TopologyController> registeredToplogies;

	private final IORedispatcher ioHandler;

	// ---------------------------------------------------
	// Public.
	// ---------------------------------------------------

	@Override
	public void submitTopology(final AuraTopology topology) {
		// sanity check.
		if (topology == null)
			throw new IllegalArgumentException("topology == null");

		if (registeredToplogies.containsKey(topology.name))
			throw new IllegalStateException("topology already submitted");

		LOG.info("TOPOLOGY '" + topology.name + "' SUBMITTED");
		registerTopology(topology).assembleTopology();
	}

	public TopologyController registerTopology(final AuraTopology topology) {
		// sanity check.
		if (topology == null)
			throw new IllegalArgumentException("topology == null");

		final TopologyController topologyController = new TopologyController(this, topology);
		registeredToplogies.put(topology.topologyID, topologyController);
		return topologyController;
	}

	public void unregisterTopology(final UUID topologyID) {
		// sanity check.
		if (topologyID == null)
			throw new IllegalArgumentException("topologyID == null");

		if (registeredToplogies.remove(topologyID) == null)
			throw new IllegalStateException("topologyID not found");
	}

	public RPCManager getRPCManager() {
		return rpcManager;
	}

	public IOManager getIOManager() {
		return ioManager;
	}

	public InfrastructureManager getInfrastructureManager() {
		return infrastructureManager;
	}
}
