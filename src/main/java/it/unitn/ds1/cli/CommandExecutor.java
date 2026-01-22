package it.unitn.ds1.cli;

import it.unitn.ds1.NetworkManager;

import java.util.HashMap;

public class CommandExecutor {
    private final NetworkManager networkManager;

    public CommandExecutor(NetworkManager networkManager) {
        this.networkManager = networkManager;
    }

    public void execute(Command cmd) throws Exception {

        switch (cmd) {
            case Command.Sleep s ->
                    Thread.sleep(s.millis());

            case Command.Init i ->
                    networkManager.initializeNetwork(i.nodesKeys(), new HashMap<>(), true);

            case Command.Exit e ->
                    throw new CliExit();

            case Command.ClientUpdate u ->
                    networkManager.requestClientUpdate(
                            u.clientName(),
                            u.dataKey(),
                            u.dataValue(),
                            u.coordinator());

            case Command.ClientGet g ->
                    networkManager.requestClientGet(g.clientName(), g.dataKey(), g.coordinator());

            case Command.ClientCreate c ->
                    networkManager.addClient(c.clientName());

            case Command.NodeJoin j ->
                    networkManager.addNode(j.nodeKey(), j.bootstrap());

            case Command.NodeLeave l ->
                    networkManager.removeNode(l.nodeKey());

            case Command.NodeCrash c ->
                    networkManager.crashNode(c.nodeKey());

            case Command.NodeRecover r ->
                    networkManager.recoverNode(r.nodeKey(), r.bootstrap());
        }
    }
}
