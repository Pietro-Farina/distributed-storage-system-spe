package it.unitn.ds1.cli;

public sealed interface Command permits
        Command.Sleep,
        Command.Init,
        Command.Exit,
        Command.ClientUpdate,
        Command.ClientGet,
        Command.ClientCreate,
        Command.NodeJoin,
        Command.NodeLeave,
        Command.NodeCrash,
        Command.NodeRecover
{
    record Sleep(long millis) implements Command {
    }

    record Init(java.util.List<Integer> nodesKeys) implements Command {
    }

    record Exit() implements Command {}

    record ClientUpdate(String clientName, int dataKey, String dataValue, int coordinator) implements Command {
    }

    record ClientGet(String clientName, int dataKey, int coordinator) implements Command {
    }

    record ClientCreate(String clientName) implements Command {}

    record NodeJoin(int nodeKey, int bootstrap) implements Command {
    }

    record NodeLeave(int nodeKey) implements Command {
    }

    record NodeCrash(int nodeKey) implements Command {
    }

    record NodeRecover(int nodeKey, int bootstrap) implements Command {
    }
}
