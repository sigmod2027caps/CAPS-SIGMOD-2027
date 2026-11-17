package temporalindex;

import java.io.Serializable;
import java.util.Map;

// Configuration of the disorder / skew / watermark machinery of experiment E3.
// Everything comes from environment variables, read once on the client and serialized with the job (the
// TaskManager's environment is not the script's). The defaults reproduce the ordered, unskewed run of
// the released artifact: no disorder, a bounded-out-of-orderness watermark of 0, no idleness.
//
//   CAPS_PARALLELISM        parallelism of the job (default 1)
//   CAPS_SOURCE_PARALLELISM readers per source; default = CAPS_PARALLELISM. Reader j of p reads the lines
//                           i of the file with i mod p = j (each with its own watermark); 1 gives the single
//                           file reader of the released artifact
//   CAPS_SINK_PARALLELISM   parallelism of the sink (default = CAPS_PARALLELISM, one sink per last-operator subtask,
//                           chained to it). 1 gives one global sink fed by all subtasks through a shuffle
//   CAPS_DISORDER_MS        D: every record is delayed by a draw from [0, D] ms (fixed seed, a function of the
//                           line number), emitted in order of (ts + delay); the event time stays the timestamp
//   CAPS_DISORDER_SEED      seed of the delays (default 7)
//   CAPS_DELTA_MS           delta: maximum out-of-orderness of the watermark. A record at most delta older than
//                           the newest one its reader has emitted is on time (default 0)
//   CAPS_IDLE_MS            tau: a reader that has emitted nothing for tau ms of processing time calls
//                           SourceContext.markAsTemporarilyIdle() (0 = never). Flink 1.10 has no
//                           WatermarkStrategy.withIdleness (that is 1.11), so the source does it
//   CAPS_WM_INTERVAL_MS     processing-time period of the watermark assigner (default 1, i.e. a watermark about every
//                           500 records of a fast replay: close to the per-record watermark of the released artifact.
//                           A coarse period slows the window operators of a replay that runs at full speed: taxi_1
//                           without provenance takes 17.8 s at 1 ms, 23 s at 50 ms and 43 s at 1 s. The disorder
//                           experiment E3 sets it explicitly (50 ms) and varies it)
//   CAPS_SLOW_FACTOR        the reader CAPS_SLOW_SUBTASK runs this many times slower than its natural pace:
//                           after every CAPS_SLOW_BATCH records it sleeps (factor - 1) times the batch time
//   CAPS_SLOW_SUBTASK       default 0; CAPS_SLOW_BATCH default 1024
//   CAPS_ZIPF               exponent of the Zipf remapping of the dataflow's key (0 = none), CAPS_ZIPF_DOMAIN the
//                           number of keys it draws from (the dataflow's parser says what the key is)
//   CAPS_ZIPF_SEED          default 12345
//   CAPS_TRACE_DIR          if set: the sources write src_stats_*.csv and src_trace_*.csv, the late-record sinks
//                           late_op_*.csv there
//   CAPS_CHECKPOINT_MS      checkpoint interval (0 = no checkpointing, the default). With checkpointing the sources
//                           store their read position, the watermark assigners their maximum timestamp and the
//                           index sinks their two-tier index (experiment E4)
//   CAPS_RESTART_ATTEMPTS   fixed-delay restart strategy used with checkpointing: attempts (default 3) ...
//   CAPS_RESTART_DELAY_MS   ... and delay between them (default 2000)
//   CAPS_FAIL_AT            fault injection: the reader CAPS_FAIL_SOURCE (default: any source) throws once, in its
//                           first execution attempt, after it has emitted this many records in its subtask 0
//                           (0 = never)
//   CAPS_PACE_SECONDS       pace every reader so that it reads its whole file in about this many seconds of processing time
//                           (0 = as fast as possible, the default), the position p of the file at time p / size * seconds,
//                           checked for every line. For the fault-injection runs (E4), which need a job that lasts long
//                           enough for a failure to land in the middle of it, and for the paced disorder runs (E3), where
//                           the watermark must trail the data by a lag that is commensurable with delta; never in timing runs
//   CAPS_COUNT_LATE         1: count the records that missed a window or arrived behind the watermark (the
//                           MissedWindowTrigger and the late side outputs; the disorder experiment E3 needs the
//                           counts). 0, the default: the plain EventTimeTrigger and no side output, which is what
//                           the Green and GeneaLog jobs run, so that all four methods run the same operators
//   CAPS_METRICS_FILE       if set and the job runs in the in-process runtime: a metric reporter writes the
//                           per-operator counters (records in/out, late records dropped by Flink) to this file
public final class DisorderConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  public final int parallelism;
  public final int sourceParallelism;
  public final int sinkParallelism;
  public final long disorderMs;
  public final long disorderSeed;
  public final long deltaMs;
  public final long idleMs;
  public final long wmIntervalMs;
  public final double slowFactor;
  public final int slowSubtask;
  public final int slowBatch;
  public final double zipfExponent;
  public final int zipfDomain;
  public final long zipfSeed;
  public final boolean countLate;
  public final String traceDir;
  public final String metricsFile;
  public final long checkpointMs;
  public final int restartAttempts;
  public final long restartDelayMs;
  public final long failAt;
  public final long paceSeconds;
  public final String failSource;

  private DisorderConfig(Map<String, String> env) {
    parallelism = (int) getLong(env, "CAPS_PARALLELISM", 1);
    sourceParallelism = (int) getLong(env, "CAPS_SOURCE_PARALLELISM", parallelism);
    sinkParallelism = (int) getLong(env, "CAPS_SINK_PARALLELISM", parallelism);
    disorderMs = getLong(env, "CAPS_DISORDER_MS", 0);
    disorderSeed = getLong(env, "CAPS_DISORDER_SEED", 7);
    deltaMs = getLong(env, "CAPS_DELTA_MS", 0);
    idleMs = getLong(env, "CAPS_IDLE_MS", 0);
    wmIntervalMs = getLong(env, "CAPS_WM_INTERVAL_MS", 1);
    slowFactor = getDouble(env, "CAPS_SLOW_FACTOR", 1);
    slowSubtask = (int) getLong(env, "CAPS_SLOW_SUBTASK", 0);
    slowBatch = (int) getLong(env, "CAPS_SLOW_BATCH", 1024);
    zipfExponent = getDouble(env, "CAPS_ZIPF", 0);
    zipfDomain = (int) getLong(env, "CAPS_ZIPF_DOMAIN", 1000);
    zipfSeed = getLong(env, "CAPS_ZIPF_SEED", 12345);
    countLate = getLong(env, "CAPS_COUNT_LATE", 0) != 0;
    traceDir = env.getOrDefault("CAPS_TRACE_DIR", "");
    metricsFile = env.getOrDefault("CAPS_METRICS_FILE", "");
    checkpointMs = getLong(env, "CAPS_CHECKPOINT_MS", 0);
    restartAttempts = (int) getLong(env, "CAPS_RESTART_ATTEMPTS", 3);
    restartDelayMs = getLong(env, "CAPS_RESTART_DELAY_MS", 2000);
    failAt = getLong(env, "CAPS_FAIL_AT", 0);
    paceSeconds = getLong(env, "CAPS_PACE_SECONDS", 0);
    failSource = env.getOrDefault("CAPS_FAIL_SOURCE", "").trim();
    if (disorderMs < 0 || deltaMs < 0 || idleMs < 0 || wmIntervalMs < 1 || slowFactor < 1
        || slowBatch < 1 || checkpointMs < 0 || restartAttempts < 0 || restartDelayMs < 0 || failAt < 0 || paceSeconds < 0 || zipfExponent < 0 || zipfDomain < 1 || sourceParallelism < 1 || sinkParallelism < 1)
      throw new IllegalArgumentException("invalid CAPS_* disorder configuration");
  }

  public static DisorderConfig fromEnv() {
    return new DisorderConfig(System.getenv());
  }

  /** The same from an explicit map of CAPS_* variables (tests). */
  public static DisorderConfig of(Map<String, String> env) {
    return new DisorderConfig(env);
  }

  /** The Zipf sampler of the key remapping, or null if CAPS_ZIPF is 0. */
  public Zipf zipf() {
    return zipfExponent > 0 ? new Zipf(zipfDomain, zipfExponent, zipfSeed) : null;
  }

  private static long getLong(Map<String, String> env, String name, long dflt) {
    String s = env.get(name);
    return s == null || s.trim().isEmpty() ? dflt : Long.parseLong(s.trim());
  }

  private static double getDouble(Map<String, String> env, String name, double dflt) {
    String s = env.get(name);
    return s == null || s.trim().isEmpty() ? dflt : Double.parseDouble(s.trim());
  }

  @Override
  public String toString() {
    return "p=" + parallelism + " sourceP=" + sourceParallelism + " sinkP=" + sinkParallelism + " D=" + disorderMs + " delta=" + deltaMs
        + " idle=" + idleMs + " wmInterval=" + wmIntervalMs + " slow=" + slowFactor + "@" + slowSubtask
        + " zipf=" + zipfExponent + "/" + zipfDomain + " countLate=" + countLate + " checkpoint=" + checkpointMs + " failAt=" + failAt;
  }
}
