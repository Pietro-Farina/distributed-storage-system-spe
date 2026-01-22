package it.unitn.ds1.protocol;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import it.unitn.ds1.DataItem;
import it.unitn.ds1.utils.OperationUid;

import java.util.Set;

public class Operation {
    public final int dataKey;
    public final QuorumTracker quorumTracker;
    public final ActorRef client;
    public final String operationType;
    public DataItem chosenVersion;
    public Cancellable timer;
    public final String proposedValue;
    public final OperationUid operationUid;

    public Operation(int dataKey, Set<Integer> expected, int requiredQuorum,
                     ActorRef client, String operationType, String proposedValue, OperationUid operationUid) {
        this.dataKey = dataKey;
        this.operationUid = operationUid;
        this.quorumTracker = new QuorumTracker(expected, requiredQuorum);
        this.client = client;
        this.operationType = operationType;
        this.proposedValue = proposedValue;
    }

    public void onOkResponse(int nodeKey, DataItem item) {
        quorumTracker.onOk(nodeKey);

        // Do NOT poison chosenVersion with null replies
        if (item == null) return;

        if (chosenVersion == null || item.getVersion() > chosenVersion.getVersion()) {
            chosenVersion = item;
        }
    }

    public void onBusyResponse(int nodeKey) {
        quorumTracker.onBusy(nodeKey);
    }
}