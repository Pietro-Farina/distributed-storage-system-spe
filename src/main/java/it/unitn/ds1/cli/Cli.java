package it.unitn.ds1.cli;

import java.io.*;
import java.util.Optional;

public final class Cli {

    public static void run(Reader input, CommandExecutor executor)
            throws Exception {

        BufferedReader br = new BufferedReader(input);
        String line;

        try {
            while ((line = br.readLine()) != null) {
                try {
                    Optional<Command> cmd = CommandParser.parse(line);
                    if (cmd.isPresent()) {
                        executor.execute(cmd.get());
                    }
                } catch (CliExit e) {
                    System.out.println("CLI terminated by user.");
                    break;
                } catch (Exception e) {
                    System.err.println("CLI error: " + e.getMessage());
                }
            }
        } finally  {
            br.close();
        }
    }
}
