package it.unitn.ds1;

import akka.actor.*;
import it.unitn.ds1.actors.Client;
import it.unitn.ds1.actors.ExperimentCoordinator;
import it.unitn.ds1.actors.Node;
import it.unitn.ds1.protocol.Messages;
import it.unitn.ds1.utils.ApplicationConfig;

import java.util.*;

public class NetworkManager {
    private final ActorSystem system;
    public final NavigableMap<Integer, ActorRef> network;
    public final Set<Integer> crashedNodes;
    public final Map<String, ActorRef> clients;
    private final ActorRef inbox;
    private Boolean initialized;

    // ------------- for events purposes ------------- //
    private final ActorRef experimentCoordinator;
    private final boolean simulation = true;
    // ------------------------------------------------ //

    // replication parameters -> Gave to the
    public final ApplicationConfig parameters;

    public NetworkManager(ApplicationConfig parameters) {
        system = ActorSystem.create("distributed-storage-system");
        network = new TreeMap<>();
        crashedNodes = new HashSet<>();
        clients = new TreeMap<>();
        this.parameters = parameters;
        this.inbox = system.actorOf(ManagerInbox.props(this), "networkManagerInbox");
        this.initialized = false;
        this.experimentCoordinator = system.actorOf(
                ExperimentCoordinator.props(parameters, inbox, parameters.random.seed),
                "experimentCoordinator"
        );
    }

    /**
     * NOTE the entries added to the network might not be deterministic
     * @param nodeKeysToAdd
     * @param dataToAdd
     */
    public void initializeNetwork(List<Integer> nodeKeysToAdd, Map<Integer, String> dataToAdd, boolean deterministic) {
        // Add nodes to the system and create the same numbers of client (for bulk updates)
        int i = 0;
        for (Integer nodeKey : nodeKeysToAdd) {
            network.put(nodeKey, system.actorOf(Node.props(nodeKey, parameters.replication, parameters.delays, parameters.random.seed), "node_" + nodeKey));
            String clientName =  "client" + ++i;
            clients.put(clientName, system.actorOf(Client.props(parameters.delays, parameters.replication, experimentCoordinator, parameters.random.seed, parameters), clientName));
        }

        // Send join messages to the nodes to inform them of the whole network
        Messages.JoinNetworkMsg start = new Messages.JoinNetworkMsg(network);
        for (Map.Entry<Integer, ActorRef> peer : network.entrySet()) {
            peer.getValue().tell(start, ActorRef.noSender());
        }

        this.initialized = true;
        if (simulation) {
            notifyExperimentCoordinator("INIT",  -1, true);
        }

        if (dataToAdd.isEmpty() || clients.isEmpty()) return;

        final int nClients = clients.size();

        List<Map.Entry<Integer, String>> entries = new ArrayList<>(dataToAdd.entrySet());
        List<ActorRef> clientList = new ArrayList<>(clients.values());

        for (int idx = 0; idx < entries.size(); idx++) {
            Map.Entry<Integer, String> e = entries.get(idx);
            int key = e.getKey();
            String value = e.getValue();

            ActorRef client;
            ActorRef coordinator;
            if (deterministic) {
                client = clientList.getFirst();
                coordinator = network.get(nodeKeysToAdd.getFirst());
            } else {
                client = clientList.get(idx % nClients);
                coordinator = network.get(nodeKeysToAdd.get(idx % nClients));
            }
            client.tell(new Messages.QueueUpdateMsg(key, value, coordinator), ActorRef.noSender());
        }

        if (deterministic) {
            // (Optional) Log a quick summary
            System.out.printf("Dispatched %d updates across %d clients (%d each, remainder %d).%nOperations might require some times%n",
                    entries.size(), 1, entries.size(), 0);
        } else {
            // (Optional) Log a quick summary
            System.out.printf("Dispatched %d updates across %d clients (%d each, remainder %d).%nOperations might require some times%n",
                    entries.size(), nClients, entries.size() / nClients, entries.size() % nClients);
        }
    }

    public void addNode(int newNodeKey, int bootstrapNodeKey) {
        if (!initialized) {
            System.out.printf(
                    "[Network Manager] %s Operation for nodeKey=%d failed: %s%n",
                    "JOIN", newNodeKey, "network not initialized"
            );
            return;
        }
        if (network.containsKey(newNodeKey)) {
            System.out.printf(
                    "[Network Manager] %s Operation for nodeKey=%d failed: %s%n",
                    "JOIN", newNodeKey, "node already exists in network"
            );
            return;
        }
        if (!network.containsKey(bootstrapNodeKey)) {
            System.out.printf(
                    "[Network Manager] %s Operation for nodeKey=%d failed: %s%n",
                    "JOIN", newNodeKey, "bootstrap node not in network"
            );
            return;
        }
        ActorRef newNode = system.actorOf(Node.props(newNodeKey, parameters.replication, parameters.delays, parameters.random.seed), "node_" + newNodeKey);
        newNode.tell(new Messages.StartJoinMsg(newNodeKey, network.get(bootstrapNodeKey)), ActorRef.noSender());
    }

    public void removeNode(int nodeKey) {
        if (!initialized) {
            System.out.printf(
                    "[Network Manager] %s Operation for nodeKey=%d failed: %s%n",
                    "LEAVE", nodeKey, "network not initialized"
            );
            return;
        }
        if (!network.containsKey(nodeKey)) {
            System.out.printf(
                    "[Network Manager] %s Operation for nodeKey=%d failed: %s%n",
                    "LEAVE", nodeKey, "node not in network"
            );
            return;
        }
        ActorRef node = network.get(nodeKey);
        node.tell(new Messages.StartLeaveMsg(), ActorRef.noSender());
    }

    public void crashNode(int nodeKey) {
        if (!initialized) {
            System.out.printf(
                    "[Network Manager] %s Operation for nodeKey=%d failed: %s%n",
                    "CRASH", nodeKey, "network not initialized"
            );
            return;
        }
        if (!network.containsKey(nodeKey)) {
            System.out.printf(
                    "[Network Manager] %s Operation for nodeKey=%d failed: %s%n",
                    "CRASH", nodeKey, "node not in network"
            );
            return;
        }
        if (crashedNodes.contains(nodeKey)) {
            System.out.printf(
                    "[Network Manager] %s Operation for nodeKey=%d failed: %s%n",
                    "RECOVER", nodeKey, "node already crashed"
            );
            return;
        }
        ActorRef node = network.get(nodeKey);
        node.tell(new Messages.CrashMsg(), ActorRef.noSender());
    }

    public void recoverNode(int nodeKey, int bootstrapNodeKey) {
        if (!initialized) {
            System.out.printf(
                    "[Network Manager] %s Operation for nodeKey=%d failed: %s%n",
                    "RECOVER", nodeKey, "network not initialized"
            );
            return;
        }
        if (!network.containsKey(nodeKey)) {
            System.out.printf(
                    "[Network Manager] %s Operation for nodeKey=%d failed: %s%n",
                    "RECOVER", nodeKey, "node not in network"
            );
            return;
        }
        if (!crashedNodes.contains(nodeKey)) {
            System.out.printf(
                    "[Network Manager] %s Operation for nodeKey=%d failed: %s%n",
                    "RECOVER", nodeKey, "node not crashed"
            );
            return;
        }
        if (!network.containsKey(bootstrapNodeKey)) {
            System.out.printf(
                    "[Network Manager] %s Operation for nodeKey=%d failed: %s%n",
                    "RECOVER", bootstrapNodeKey, "bootstrap node [" + bootstrapNodeKey +"] not in network"
            );
            return;
        }
        ActorRef node = network.get(nodeKey);
        node.tell(new Messages.StartRecoveryMsg(
                network.get(bootstrapNodeKey)
        ), ActorRef.noSender());
    }

    public void requestClientUpdate(String clientName, int dataKey, String dataValue, int nodeKey) {
        if (!initialized) {
            System.out.printf(
                    "[Network Manager] %s Operation from client=%s for nodeKey=%d failed: %s%n",
                    "CLIENT_UPDATE", clientName, nodeKey, "network not initialized"
            );
            return;
        }
        if (!clients.containsKey(clientName)) {
            System.out.printf(
                    "[Network Manager] %s Operation from client=%s for nodeKey=%d failed: %s%n",
                    "CLIENT_UPDATE", clientName, nodeKey, "client not in system"
            );
            return;
        }
        if (!network.containsKey(nodeKey)) {
            System.out.printf(
                    "[Network Manager] %s Operation from client=%s for nodeKey=%d failed: %s%n",
                    "CLIENT_UPDATE", clientName, nodeKey, "node not in network"
            );
            return;
        }
        ActorRef client = clients.get(clientName);
        ActorRef node = network.get(nodeKey);

        client.tell(new Messages.StartUpdateMSg(
                dataKey, dataValue, node
        ),  ActorRef.noSender());
    }

    public void requestClientGet(String clientName, int dataKey, int nodeKey) {
        if (!initialized) {
            System.out.printf(
                    "[Network Manager] %s Operation from client=%s for nodeKey=%d failed: %s%n",
                    "CLIENT_GET", clientName, nodeKey, "network not initialized"
            );
            return;
        }
        if (!clients.containsKey(clientName)) {
            System.out.printf(
                    "[Network Manager] %s Operation from client=%s for nodeKey=%d failed: %s%n",
                    "CLIENT_GET", clientName, nodeKey, "client not in system"
            );
            return;
        }
        if (!network.containsKey(nodeKey)) {
            System.out.printf(
                    "[Network Manager] %s Operation from client=%s for nodeKey=%d failed: %s%n",
                    "CLIENT_GET", clientName, nodeKey, "node not in network"
            );
            return;
        }
        ActorRef client = clients.get(clientName);
        ActorRef node = network.get(nodeKey);

        client.tell(new Messages.StartGetMsg(
                dataKey, node
        ),  ActorRef.noSender());
    }

    public void addClient(String clientName) {
        if (!initialized) {
            System.out.printf(
                    "[Network Manager] %s Operation for client=%s failed: %s%n",
                    "ADD_CLIENT", clientName, "network not initialized"
            );
            return;
        }
        if (clients.containsKey(clientName)) {
            System.out.printf(
                    "[Network Manager] %s Operation for client=%s failed: %s%n",
                    "ADD_CLIENT", clientName, "client already in network"
            );
            return;
        }
        clients.put(clientName,
            system.actorOf(Client.props(parameters.delays, parameters.replication, experimentCoordinator, parameters.random.seed, parameters), clientName)
        );
        notifyExperimentCoordinator("NEW_CLIENT",  -1, true);
        System.out.printf(
                "[Network Manager] %s Operation for client=%s succeed%n",
                "ADD_CLIENT", clientName
        );
    }

    public void removeClient(String clientName) {
        if (!initialized) {
            System.out.printf(
                    "[Network Manager] %s Operation for client=%s failed: %s%n",
                    "REMOVE_CLIENT", clientName, "network not initialized"
            );
            return;
        }
        if (!clients.containsKey(clientName)) {
            System.out.printf(
                    "[Network Manager] %s Operation for client=%s failed: %s%n",
                    "REMOVE_CLIENT", clientName, "client not in network"
            );
            return;
        }
        ActorRef client = clients.get(clientName);
        system.stop(client);
        clients.remove(clientName);
        System.out.printf(
                "[Network Manager] %s Operation for client=%s succeed%n",
                "RECOVER", clientName
        );
    }


    public void terminate() {
        system.terminate();
    }

    public void startExperiment() {
        if (!initialized) {
            System.out.printf(
                    "[Network Manager] %s Operation failed: %s%n",
                    "START_EXPERIMENT", "network not initialized"
            );
            return;
        }
        notifyExperimentCoordinator("START_EXPERIMENT", -1, true);
    }

    public void stopExperiment() {
        notifyExperimentCoordinator("STOP_EXPERIMENT", -1, true);
    }

    // ------------ HELPERS FOR HAVING UPDATED RING ON DATA MANAGER ------------ //
    private void notifyExperimentCoordinator(String operationType, int nodeKey, boolean success) {
        if (simulation) {
            experimentCoordinator.tell(
                    new Messages.ResultMembershipOperationMsg(
                            this.network,
                            this.clients,
                            operationType,
                            nodeKey,
                            success
                    ),
                    ActorRef.noSender()
            );
        }
    }
    /**
     * The following functions are implemented as follows since we are under the assumptions that
     * "Nodes join and leave, crash and recover one at a time, and only when there are no ongoing operations".
     * If it wasn't for that, then this implementation could lead to inconsistency due to race conditions.
     */

    // called only by the inbox actor (single-threaded), but keep synchronized for safety
    synchronized void onJoin(int key, ActorRef ref) {
        network.put(key, ref);
        notifyExperimentCoordinator("JOIN", key, true);
    }
    synchronized void onLeave(int key) {
        ActorRef node = network.remove(key);
        system.stop(node);
        notifyExperimentCoordinator("LEAVE", key, true);
    }
    synchronized void onCrash(int key) { /* keep membership; optionally track a status map */
        crashedNodes.add(key);
        notifyExperimentCoordinator("CRASH", key, true);
    }
    synchronized void onRecover(int key, ActorRef ref) {
        crashedNodes.remove(key);
        notifyExperimentCoordinator("RECOVER", key, true);
    }

    synchronized void onRequestMembershipOperationMsg(Messages.RequestMembershipOperationMsg msg) {
        switch (msg.operationType) {
            case "JOIN" -> addNode(msg.nodeKey, msg.bootstrapNodeKey);
            case "LEAVE" -> removeNode(msg.nodeKey);
            case "RECOVER" -> recoverNode(msg.nodeKey, msg.bootstrapNodeKey);
            case "CRASH" -> crashNode(msg.nodeKey);
            case "TERMINATE_EXPERIMENT" -> {}
            default -> System.out.printf(
                    "[Network Manager] %s Operation failed: %s%n",
                    msg.operationType, "operation not supported"
            );
        }
    }

    // for your helpers/UI/tests
    public synchronized NavigableMap<Integer, ActorRef> snapshot(){ return new TreeMap<>(network); }

    public static class ManagerInbox extends AbstractActor {
        private final NetworkManager networkManager;

        private ManagerInbox(NetworkManager networkManager) {
            this.networkManager = networkManager;
        }
        public static Props props(NetworkManager networkManager) {
            return Props.create(ManagerInbox.class, () -> new ManagerInbox(networkManager));
        }

        @Override public Receive createReceive(){
            return receiveBuilder()
                    .match(Messages.ManagerNotifyJoin.class,m -> networkManager.onJoin(m.nodeKey, sender()))
                    .match(Messages.ManagerNotifyLeave.class,m -> networkManager.onLeave(m.nodeKey))
                    .match(Messages.ManagerNotifyCrash.class,m -> networkManager.onCrash(m.nodeKey))
                    .match(Messages.ManagerNotifyRecover.class,m -> networkManager.onRecover(m.nodeKey, sender()))
                    .match(Messages.RequestMembershipOperationMsg.class, networkManager::onRequestMembershipOperationMsg)
                    .build();
        }
    }
    // ------------------------------------------------------------------------- //
}
