package it.unitn.ds1.utils;

import java.util.Arrays;
import java.util.SplittableRandom;

public class DistributionRandomGenerator {
    // root only used to generate child streams deterministically at init
    private final SplittableRandom root;

    // one independent stream per distribution family
    private final SplittableRandom uni;
    private final SplittableRandom weighted;
    private final SplittableRandom bern;
    private final SplittableRandom pois;
    private final SplittableRandom zipf;
    private final SplittableRandom expo;
    private final SplittableRandom normal; // helper stream

    // Zipf CDf
    private ZipfSampler  zipfSampler;

    public DistributionRandomGenerator(long runSeed, String actorId) {
        if (actorId == null) throw new IllegalArgumentException("actorId must not be null");

        long baseSeed = mix64(runSeed ^ (long) actorId.hashCode());
        this.root = new SplittableRandom(baseSeed);

        // Split ONCE at construction. Order must never change to preserve reproducibility.
        this.uni      = root.split();
        this.weighted = root.split();
        this.bern     = root.split();
        this.pois     = root.split();
        this.zipf     = root.split();
        this.expo     = root.split();
        this.normal   = root.split();
    }

    // -------------------- Uniform --------------------

    /** U[0,1) */
    public double uniform01() {
        return uni.nextDouble();
    }

    /** U[a,b) */
    public double uniform(double a, double b) {
        if (!(b > a)) throw new IllegalArgumentException("b must be > a");
        return a + (b - a) * uni.nextDouble();
    }

    /** Uniform integer in [0, bound) */
    public int uniformInt(int bound) {
        if (bound <= 0) throw new IllegalArgumentException("bound must be > 0");
        return uni.nextInt(bound);
    }

    /** Uniform integer in [min, max] inclusive */
    public int uniformIntInclusive(int min, int max) {
        if (max < min) throw new IllegalArgumentException("max must be >= min");
        long span = (long) max - (long) min + 1L;
        if (span <= 0 || span > Integer.MAX_VALUE) throw new IllegalArgumentException("range too large");
        return min + uni.nextInt((int) span);
    }

    // -------------------- Weighted categorical --------------------

    /** Sample index i with probability proportional to weights[i]. O(n). */
    public int weightedIndex(double[] weightsArr) {
        if (weightsArr == null || weightsArr.length == 0) throw new IllegalArgumentException("empty weights");
        double total = 0.0;
        for (double w : weightsArr) {
            if (w < 0.0) throw new IllegalArgumentException("negative weight");
            total += w;
        }
        if (!(total > 0.0)) throw new IllegalArgumentException("all weights are zero");

        double r = weighted.nextDouble() * total;
        double acc = 0.0;
        for (int i = 0; i < weightsArr.length; i++) {
            acc += weightsArr[i];
            if (r < acc) return i;
        }
        return weightsArr.length - 1; // numeric fallthrough
    }

    public <T> T weightedChoice(T[] values, double[] weightsArr) {
        if (values == null || weightsArr == null) throw new IllegalArgumentException("null args");
        if (values.length != weightsArr.length) throw new IllegalArgumentException("values/weights length mismatch");
        return values[weightedIndex(weightsArr)];
    }


    // -------------------- Bernoulli --------------------

    /** Bernoulli(p): true with probability p */
    public boolean bernoulli(double p) {
        if (!(p >= 0.0 && p <= 1.0)) throw new IllegalArgumentException("p must be in [0,1]");
        return bern.nextDouble() < p;
    }

    // -------------------- Exponential / shifted exponential --------------------

    /** Exp(lambda): mean 1/lambda */
    public double exponential(double lambda) {
        if (!(lambda > 0.0)) throw new IllegalArgumentException("lambda must be > 0");
        double u = expo.nextDouble();            // u in [0,1)
        return -Math.log1p(-u) / lambda;         // stable for small u
    }

    /** shift + Exp(lambda) */
    public double shiftedExponential(double shift, double lambda) {
        if (!(shift >= 0.0)) throw new IllegalArgumentException("shift must be >= 0");
        return shift + exponential(lambda);
    }

    public long exponentialDelayMs(double lambdaPerMs, long minMs, long maxMs) {
        double x = exponential(lambdaPerMs);          // in ms
        return toBoundedDelayMs(x, minMs, maxMs);
    }

    public long shiftedExponentialDelayMs(long shiftMs, double lambdaPerMs, long maxMs) {
        double x = shiftedExponential((double) shiftMs, lambdaPerMs); // in ms
        return toBoundedDelayMs(x, shiftMs, maxMs);
    }

    // -------------------- Poisson process --------------------

    /** Next inter-arrival time for Poisson process with rate lambda (Exp(lambda)). */
    public double poissonInterArrival(double lambda) {
        if (!(lambda > 0.0)) throw new IllegalArgumentException("lambda must be > 0");
        double u = pois.nextDouble();
        return -Math.log1p(-u) / lambda;
    }

    /** Events count in window T for Poisson process with rate lambda: Poisson(mu=lambda*T). */
    public int poissonInWindow(double lambda, double windowT) {
        if (!(lambda >= 0.0)) throw new IllegalArgumentException("lambda must be >= 0");
        if (!(windowT >= 0.0)) throw new IllegalArgumentException("windowT must be >= 0");
        return poisson(lambda * windowT);
    }


    public long poissonInterArrivalMs(double lambdaPerMs, long minMs, long maxMs) {
        double x = poissonInterArrival(lambdaPerMs);  // in ms
        return toBoundedDelayMs(x, minMs, maxMs);
    }

    // -------------------- Bounded Delays for exp and poisson --------------------
    private static long toBoundedDelayMs(double xMs, long minMs, long maxMs) {
        if (Double.isNaN(xMs) || Double.isInfinite(xMs)) throw new IllegalStateException("invalid delay");
        // Rounding rule: ceil ensures non-zero delays when xMs is small but positive.
        long d = (long) Math.ceil(xMs);
        if (d < minMs) d = minMs;
        if (maxMs > 0 && d > maxMs) d = maxMs;
        return d;
    }

    // -------------------- Poisson distribution --------------------

    /**
     * Poisson(mu):
     * - Knuth for mu <= 30
     * - Normal approximation for mu > 30 (fast; not exact)
     */
    public int poisson(double mu) {
        if (!(mu >= 0.0)) throw new IllegalArgumentException("mu must be >= 0");
        if (mu == 0.0) return 0;

        if (mu <= 30.0) {
            double L = Math.exp(-mu);
            int k = 0;
            double p = 1.0;
            do {
                k++;
                p *= pois.nextDouble();
            } while (p > L);
            return k - 1;
        }

        double z = standardNormal();
        long n = Math.round(mu + Math.sqrt(mu) * z);
        return (int) Math.max(0L, n);
    }

    // -------------------- Zipf (finite support) --------------------

    /** Zipf over ranks 1..n with exponent s: P(k) ∝ 1/k^s. Returns rank in [1..n]. */
    public int zipf(int n, double s) {
        if (zipfSampler == null || (zipfSampler.n != n || zipfSampler.s != s)) {
            zipfSampler = new ZipfSampler(n, s);
        }
        return zipfSampler.sample(zipf);
    }

    public static final class ZipfSampler {
        private final int n;
        private final double s;
        private final double[] cdf; // index 0..n-1 corresponds to rank 1..n

        public ZipfSampler(int n, double s) {
            if (n <= 0) throw new IllegalArgumentException("n must be > 0");
            if (!(s > 0.0)) throw new IllegalArgumentException("s must be > 0");
            this.n = n;
            this.s = s;
            this.cdf = new double[n];

            double sum = 0.0;
            for (int k = 1; k <= n; k++) sum += 1.0 / Math.pow(k, s);

            double acc = 0.0;
            for (int k = 1; k <= n; k++) {
                acc += (1.0 / Math.pow(k, s)) / sum;
                cdf[k - 1] = acc;
            }
            cdf[n - 1] = 1.0;
        }

        public int sample(SplittableRandom rng) {
            double u = rng.nextDouble();
            int idx = Arrays.binarySearch(cdf, u);
            if (idx < 0) idx = -idx - 1;
            return idx + 1;
        }

        public int n() { return n; }
        public double s() { return s; }
    }


    // -------------------- Normal helper --------------------

    /** Standard normal N(0,1) using Box-Muller (draws from dedicated 'normal' stream). */
    public double standardNormal() {
        double u1 = 1.0 - normal.nextDouble(); // (0,1]
        double u2 = normal.nextDouble();       // [0,1)
        return Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math.PI * u2);
    }

    // -------------------- Seed mixing --------------------

    private static long mix64(long z) {
        z += 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}
