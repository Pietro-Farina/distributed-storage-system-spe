package it.unitn.ds1.cli;

import java.util.*;

public final class CommandParser {

    public static Optional<Command> parse(String line) {
        line = line.trim();
        if (line.isEmpty() || line.startsWith("#")) return Optional.empty();

        String[] tokens = line.split("\\s+");

        return switch (tokens[0].toLowerCase()) {

            // example ask client to get data 30 to node 20: client 30 20
            case "sleep" ->
                    Optional.of(new Command.Sleep(Long.parseLong(tokens[1])));

            // example initialize the network: init
            case "init" ->
                    Optional.of(new Command.Init(
                            Arrays.stream(tokens).skip(1)
                                    .map(Integer::parseInt)
                                    .toList()
                    ));

            case "exit", "quit" ->
                Optional.of(new Command.Exit());

            // Node distributed operations: join, leave, crash, recover
            case "node" ->
                switch (tokens[1].toLowerCase()) {
                    // example: ask node 10 to join network with node 20 as bootstrap: node join 10 20
                    case "join" ->
                            Optional.of(new Command.NodeJoin(
                                    Integer.parseInt(tokens[2]), Integer.parseInt(tokens[3])
                            ));

                    // example: ask node 10 to leave the network: node leave 10
                    case "leave" ->
                            Optional.of(new Command.NodeLeave(Integer.parseInt(tokens[2])));

                    // example: ask node 10 to crash: node crash 10
                    case "crash" ->
                            Optional.of(new Command.NodeCrash(Integer.parseInt(tokens[2])));

                    // example: ask node 10 to recover with node 20 as bootstrap: node recover 10 20
                    case "recover" ->
                            Optional.of(new Command.NodeRecover(
                                    Integer.parseInt(tokens[2]), Integer.parseInt(tokens[3])
                            ));

                    default ->
                            throw new IllegalArgumentException("Unknown command: " + tokens[0] + " " + tokens[1]);
                };


            // Client distributed operations: update, get
            // Client handling operations (for network manager): create
            case "client" ->
                switch (tokens[1].toLowerCase()) {
                    // example: ask client to update data 30 with test through node 20: client update 30 test 20
                    case "update" ->
                            Optional.of(new Command.ClientUpdate(
                                    tokens[2].toLowerCase(),
                                    Integer.parseInt(tokens[3]),
                                    tokens[4],
                                    Integer.parseInt(tokens[5])
                            ));

                    // example: ask client to get data 30 from node 20: client get 30 20
                    case "get" ->
                            Optional.of(new Command.ClientGet(
                                    tokens[2].toLowerCase(),
                                    Integer.parseInt(tokens[3]),
                                    Integer.parseInt(tokens[4])
                            ));

                    // example: ask the manager to create client to perform operations: client create clientName
                    case "create" ->
                        Optional.of(new Command.ClientCreate(
                                tokens[2].toLowerCase()
                        ));

                    default ->
                            throw new IllegalArgumentException("Unknown command: " + tokens[0] + " " + tokens[1]);
                };

            default ->
                    throw new IllegalArgumentException("Unknown command: " + tokens[0]);
        };
    }
}