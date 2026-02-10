package it.unitn.ds1.utils;

import com.typesafe.config.*;
import it.unitn.ds1.logging.LoggerConfig;

import java.time.Duration;
import java.util.Objects;

public class ApplicationConfig {
    public final Replication replication;
    public final Delays delays;
    public final Ring ring;
    public final Paths paths;
    public final RandomSeed random;
    public final Log log;

    private ApplicationConfig(Replication replication, Delays delays, Ring ring, Paths paths,
                       RandomSeed random, Log log) {
        this.replication = replication;
        this.delays = delays;
        this.ring = ring;
        this.paths = paths;
        this.random = random;
        this.log = log;
        validate();
    }

    public static ApplicationConfig from(Config root) {
        Config c = root.getConfig("ds1");
        return new ApplicationConfig(
                Replication.from(c.getConfig("replication")),
                Delays.from(c.getConfig("delays")),
                Ring.from(c.getConfig("ring")),
                Paths.from(c.getConfig("paths")),
                RandomSeed.from(c.getConfig("random")),
                Log.from(c.getConfig("log"))
        );
    }

    private void validate() {
        // Basic invariants commonly used with quorums
        if (replication.N <= 0) throw new IllegalArgumentException("N must be > 0");
        if (replication.R <= 0 || replication.W <= 0) throw new IllegalArgumentException("R,W must be > 0");
        if (replication.R > replication.N || replication.W > replication.N)
            throw new IllegalArgumentException("R and W must be ≤ N");
        // Quorum operations requirement
        if (replication.R + replication.W <= replication.N)
            System.err.println("[WARN] R + W must be > N to avoid read/write conflicts");
        if (replication.W * 2 <= replication.N)
            System.err.println("[WARN] W must be > N/2 to avoid write/write conflicts");

        if (delays.delayMinMs < 0 || delays.delayMaxMs < delays.delayMinMs)
            throw new IllegalArgumentException("Invalid delay range");

        if (ring.keySpace <= 0) throw new IllegalArgumentException("keySpace must be > 0");

        if (log.eventSample < 0.0 || log.eventSample > 1.0)
            throw new IllegalArgumentException("eventSample must be in [0,1]");
    }

    // ------- nested sections --------
    public static final class Replication {
        public final int N, R, W, T;

        private Replication(int n, int r, int w, int t) {
            this.N = n; this.R = r; this.W = w; this.T = t;
        }
        public static Replication from(Config c) {
            int n = c.getInt("N");
            int r = c.getInt("R");
            int w = c.getInt("W");
            int t = c.getInt("T_ms");
            return new Replication(n, r, w, t);
        }
    }

    public static final class Delays {
        public final long delayMinMs, delayMaxMs, shiftMs, tailMs;
        public final double lambdaPerMs;
        private Delays(long min, long max, long shiftMs, long tailMs, double lambdaPerMs) {
            this.delayMinMs = min; this.delayMaxMs = max; this.shiftMs = shiftMs; this.tailMs = tailMs;
            this.lambdaPerMs = lambdaPerMs; }
        public static Delays from(Config c) {
            return new Delays(c.getLong("delayMinMs"), c.getLong("delayMaxMs"), c.getLong("shiftMs"), c.getLong("tailMs"), c.getDouble("exponentialLambdaPerMsNumerator") / c.getDouble("exponentialLambdaPerMsDenominator"));
        }
    }

    public static final class Ring {
        public final int keySpace;
        public final long membershipScheduling;
        public final int nrNodes, nrClients;
        private Ring(int keySpace, long membershipScheduling, int nrNodes, int nrClients) { this.keySpace = keySpace; this.membershipScheduling = membershipScheduling;
            this.nrNodes = nrNodes;
            this.nrClients = nrClients;
        }
        public static Ring from(Config c) { return new Ring(c.getInt("keySpace"), c.getInt("membershipScheduling"), c.getInt("nrNodes"), c.getInt("nrClients") ); }
    }

    public static final class Paths {
        public final String runsDir, dataDir;
        private Paths(String runsDir, String dataDir) { this.runsDir = runsDir; this.dataDir = dataDir; }
        public static Paths from(Config c) { return new Paths(c.getString("runsDir"), c.getString("dataDir")); }
    }

    public static final class RandomSeed {
        public final long seed;
        public final double zipfSkew, readP, lambdaPerSec;
        private RandomSeed(long seed, double zipfSkew, double readP, double lambdaPerSec) { this.seed = seed; this.zipfSkew = zipfSkew;  this.readP = readP; this.lambdaPerSec = lambdaPerSec; }
        public static RandomSeed from(Config c) { return new RandomSeed(c.getLong("seed"), c.getDouble("zipfSkew"), c.getDouble("readP"), c.getDouble("poissonLambdaOpsPerSec")); }
    }

    public static final class Log {
        public final LoggerConfig.EventDetail eventDetail;
        public final String runTag;
        public final boolean summaryEnabled;
        public final double eventSample;
        public final int batchSize;
        public final Duration flushEvery;
        public final boolean logToConsole, writeParanoid;
        public final String runID;

        private Log(LoggerConfig.EventDetail ed, String runTag, boolean summaryEnabled, double eventSample,
                    int batchSize, Duration flushEvery, boolean logToConsole, boolean writeParanoid, String runID) {
            this.eventDetail = Objects.requireNonNull(ed);
            this.runTag = runTag;
            this.summaryEnabled = summaryEnabled;
            this.eventSample = eventSample;
            this.batchSize = batchSize;
            this.flushEvery = flushEvery;
            this.logToConsole = logToConsole;
            this.writeParanoid = writeParanoid;
            this.runID = runID;
        }

        public static Log from(Config c) {
            return new Log(
                    c.getEnum(LoggerConfig.EventDetail.class, "eventDetail"),
                    c.getString("runTag"),
                    c.getBoolean("summaryEnabled"),
                    c.getDouble("eventSample"),
                    c.getInt("batchSize"),
                    Duration.ofMillis(c.getInt("flushEvery")),
                    c.getBoolean("logToConsole"),
                    c.getBoolean("writeParanoid"),
                    c.getString("runID")
            );
        }
    }
}
