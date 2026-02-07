package it.unitn.ds1.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.protocol.Messages;
import it.unitn.ds1.utils.ApplicationConfig;
import it.unitn.ds1.utils.DistributionRandomGenerator;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class ExperimentCoordinator extends AbstractActor {
    private final ApplicationConfig parameters;
    private final ActorRef managerInbox;
    private final NavigableMap<Integer, ActorRef> currentNodes;
    private final Map<String, ActorRef> currentClients;
    private final Set<Integer> crashedNodes;
    private final Set<String> idleClients;
    private boolean running;

    // used to track the scheduled maintenance phase
    private boolean maintenance;

    // Random Factory
    private final DistributionRandomGenerator random;

    public static Props props(ApplicationConfig parameters, ActorRef managerInbox, long runSeed) {
        return Props.create(ExperimentCoordinator.class, () -> new ExperimentCoordinator(parameters, managerInbox, runSeed));
    }

    private ExperimentCoordinator(ApplicationConfig parameters, ActorRef managerInbox, long runSeed) {
        this.parameters = parameters;
        this.managerInbox = managerInbox;
        this.currentNodes = new TreeMap<>();
        this.currentClients = new HashMap<>();
        this.crashedNodes = new HashSet<>();
        this.idleClients = new HashSet<>();
        this.maintenance = false;
        this.random = new DistributionRandomGenerator(runSeed, self().path().name());
    }

    private void onSystemQuiescent() {
        // sample the operation
        Set<String> operationType = new HashSet<>();
        operationType.add("CRASH");
        if (!crashedNodes.isEmpty()) {
            operationType.add("RECOVER");
        }
        if (currentNodes.size() < parameters.ring.keySpace) {
            operationType.add("JOIN");
        }
        if (currentNodes.size() > parameters.replication.N) {
            operationType.add("LEAVE");
        }

        // Weight the operation probabilities
        String[] operations = operationType.toArray(new String[0]);
        double[] initialWeights = new double[operations.length];
        java.util.Arrays.fill(initialWeights, 1.0);

        String selectedOperation = random.weightedChoice(operations, initialWeights);

        // select valid node
        int nodeKey;
        int bootstrapNodeKey;
        if (selectedOperation.equals("RECOVER")) { // must be a crashed node + boostrap
            Integer[] crashedNodeKeys = crashedNodes.toArray(new Integer[0]);
            nodeKey = crashedNodeKeys[random.uniformInt(crashedNodes.size())];

            int finalNodeKey = nodeKey;
            Integer[] nodeKeys = currentNodes.keySet().stream()
                    .filter(key -> !key.equals(finalNodeKey))
                    .toArray(Integer[]::new);
            bootstrapNodeKey = nodeKeys[random.uniformInt(currentNodes.size())];
        } else if (selectedOperation.equals("JOIN")) { // must be an inactive node + boostrap
            int[] inactiveKeys = IntStream.range(0, parameters.ring.keySpace)
                    .filter(key -> !currentNodes.containsKey(key))
                    .toArray();
            nodeKey = inactiveKeys[random.uniformInt(inactiveKeys.length)];

            Integer[] nodeKeys = currentNodes.keySet().toArray(new Integer[0]);
            bootstrapNodeKey = nodeKeys[random.uniformInt(currentNodes.size())];
        } else {
            Integer[] nodeKeys = currentNodes.keySet().toArray(new Integer[0]);
            nodeKey = nodeKeys[random.uniformInt(currentNodes.size())];
            bootstrapNodeKey = -1;
        }

        // send the request to the network manager
        managerInbox.tell(
                new Messages.RequestMembershipOperationMsg(
                        selectedOperation,
                        nodeKey,
                        bootstrapNodeKey
                ),
                self()
        );
    }

    // Fixed maintenance schedule
    private void scheduleNextMembership() {
        long maintenanceDelay = 120_000L;

        getContext().system().scheduler().scheduleOnce(
                Duration.create(maintenanceDelay, TimeUnit.MILLISECONDS),
                getSelf(),
                new IssueMembershipOperation(),
                getContext().system().dispatcher(),
                getSelf()
        );
    }

    // On scheduled maintenance phase
    private void onMaintenanceStart() {
        if (maintenance) {
            return;
        } else {
            maintenance = true;
        }

        // free idleClients
        idleClients.clear();

        // multicast request to pause operation
        multicastMessageToClients(
                currentClients.keySet(),
                new Messages.PauseOperationsMsg()
        );
    }

    private void onClientPaused(Messages.ClientIdleMsg msg) {
        String clientId = getSender().path().name();
        idleClients.add(clientId);

        // we can start the membership operation
        if (idleClients.size() == currentClients.size()) {
            if (running) {
                onSystemQuiescent();
            } else {
                managerInbox.tell(
                    new Messages.RequestMembershipOperationMsg(
                        "TERMINATE_EXPERIMENT",
                        -1,
                        -1
                    ),
                    self()
                );
            }
        }
    }

    private void onNetworkOperationResult(Messages.ResultMembershipOperationMsg resultMembershipOperationMsg) {
        maintenance = false;

        if (resultMembershipOperationMsg.operationType.equals("START_EXPERIMENT")) {
            running = true;
        } else if (resultMembershipOperationMsg.operationType.equals("STOP_EXPERIMENT")) {
            running = false;
            multicastMessageToClients(
                    currentClients.keySet(),
                    new Messages.PauseOperationsMsg()
            );
            return;
        }

        // update each field if present
        if (resultMembershipOperationMsg.network != null) {
            currentNodes.clear();
            currentNodes.putAll(resultMembershipOperationMsg.network);
        }
        if (resultMembershipOperationMsg.clients != null) {
            currentClients.clear();
            currentClients.putAll(resultMembershipOperationMsg.clients);
        }
        // if successful we need to do some changes to the structure (JOIN + LEAVE already covered)
        if (resultMembershipOperationMsg.success && resultMembershipOperationMsg.nodeKey != -1) {
            if (resultMembershipOperationMsg.operationType.equals("CRASH")) {
                crashedNodes.add(resultMembershipOperationMsg.nodeKey);
            } else if (resultMembershipOperationMsg.operationType.equals("RECOVER")) {
                crashedNodes.remove(resultMembershipOperationMsg.nodeKey);
            }
        }

        // multicast resum operation to clients
        multicastMessageToClients(
                currentClients.keySet(),
                new Messages.ResumeOperationsMsg(this.currentNodes)
        );

        // set the new timed operation
        scheduleNextMembership();
    }

    private void multicastMessageToClients(Set<String> recipientNodes, Serializable msg) {
        for (String clientId: recipientNodes) {
            ActorRef node = currentClients.get(clientId);
            node.tell(msg, self());
        }
    }

    private class IssueMembershipOperation implements Serializable {}

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.ClientIdleMsg.class, this::onClientPaused)
                .match(IssueMembershipOperation.class, msg -> onMaintenanceStart())
                .match(Messages.ResultMembershipOperationMsg.class, this::onNetworkOperationResult)
                .build();
    }
}
