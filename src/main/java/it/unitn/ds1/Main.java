package it.unitn.ds1;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import it.unitn.ds1.cli.Cli;
import it.unitn.ds1.cli.CommandExecutor;
import it.unitn.ds1.logging.AsyncRunLogger;
import it.unitn.ds1.logging.LogModels;
import it.unitn.ds1.logging.LoggerConfig;
import it.unitn.ds1.utils.ApplicationConfig;

import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;

public class Main {
    final static int N_NODES = 4;

    public static void main(String[] args) throws Exception {
        // Loads application.conf (+ application-{env}.conf overrides if present)
        // Note that they can be override by command-line, e.g.:
        // -Dds1.replication.N=5 -Dds1.log.eventDetail=NORMAL
        Config raw = ConfigFactory.load();
        ApplicationConfig cfg = ApplicationConfig.from(raw);

        System.out.printf(
                "N=%d R=%d W=%d T=%dms | keySpace=%d | delay=[%d..%d]ms | runsDir=%s | tag=%s eventDetail=%s summaryEnabled=%s logToConsole=%s%n",
                cfg.replication.N, cfg.replication.R, cfg.replication.W, cfg.replication.T,
                cfg.ring.keySpace, cfg.delays.delayMinMs, cfg.delays.delayMaxMs,
                cfg.paths.runsDir, cfg.log.runTag, cfg.log.eventDetail, cfg.log.summaryEnabled, cfg.log.logToConsole
        );

        // Random Generator
        Random rand = new Random(cfg.random.seed);

        // Initialize logger in (main or SimulationRunner)
        var loggerCfg = new LoggerConfig.Builder()
                .runTag(cfg.log.runTag)
                .baseDir(java.nio.file.Paths.get(cfg.paths.runsDir))
                .eventDetail(cfg.log.eventDetail)
                .summaryEnabled(cfg.log.summaryEnabled)
                .eventSample(cfg.log.eventSample)
                .batchSize(cfg.log.batchSize)
                .flushEvery(cfg.log.flushEvery)
                .logToConsole(cfg.log.logToConsole)
                .writeParanoid(cfg.log.writeParanoid)
                .build();
        var meta = new LogModels.Metadata(
                java.time.Instant.now().toString(),
                "test1",      // not used; kept for completeness
                loggerCfg.runTag,
                cfg.replication.N, cfg.replication.R, cfg.replication.W, (int) cfg.replication.T,                  // your run params
                10, 10,
                cfg.delays.shiftMs, cfg.delays.tailMs, cfg.delays.lambdaPerMs,
                cfg.ring.keySpace, cfg.ring.membershipScheduling, cfg.ring.nrNodes, cfg.ring.nrClients,
                cfg.random.seed, cfg.random.zipfSkew, cfg.random.readP, cfg.random.lambdaPerSec,
                System.getProperty("java.version")
        );
        var logger = AsyncRunLogger.start(loggerCfg, meta);

        // Create the Network Manager
        NetworkManager networkManager = new NetworkManager(cfg);

        // Create the command executor ( -> reads and send request to network manager)
        CommandExecutor executor = new CommandExecutor(networkManager);

        Reader input;
        if (args.length == 0) {
            System.out.println("Interactive CLI mode");
            input = new InputStreamReader(System.in);
        } else if (args.length == 1) {
            System.out.println("Scripted CLI mode: " + args[0]);
            input = new FileReader(args[0]);
        } else {
            throw new IllegalArgumentException(
                    "Usage: gradlew run [script.txt]"
            );
        }

        Cli.run(input, executor);

        networkManager.terminate();
    }
}
