package temporalindex;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

// Wiring shared by the CAPS and the no-provenance jobs (and so identical in both): the environment, the parallel
// disordered source with its watermark assigner, the side outputs for late records, the end-of-job report.
//
// Watermarks: Flink 1.10's BoundedOutOfOrdernessTimestampExtractor, emitted periodically (processing time,
// CAPS_WM_INTERVAL_MS). Its watermark is (newest timestamp - maxOutOfOrderness), i.e. it promises that no record
// with a timestamp <= that follows. The model of the paper says a record at most delta older than the newest one is
// on time, so the extractor is given delta + 1 (1.11 and later subtract the 1 themselves). With delta = 0 and an
// ordered input this is a watermark one millisecond below the newest timestamp.
// With CAPS_CHECKPOINT_MS > 0 the job checkpoints (see DisorderSource, Assigner and caps.TieredIndexSink for what each
// part stores) and restarts from the last completed checkpoint after a failure.
// Allowed lateness is 0 everywhere, so a window fires once. With CAPS_COUNT_LATE=1 (experiment E3 only) a record whose
// windows have all fired goes to the side output of its window operator (sideOutputLateData) and is counted there, and
// the MissedWindowTrigger counts the records that missed some of their windows; without it (the default, and what every
// timing experiment runs) the window operators have the default EventTimeTrigger and no side output. Flink 1.10's window join has no
// sideOutputLateData: late records of a join are dropped by the operator and counted by its numLateRecordsDropped
// metric (CAPS_METRICS_FILE, in-process runtime; through the REST API on a cluster).
public final class Disorder {

  private Disorder() {}

  private static final long BASE_EPOCH_US = System.currentTimeMillis() * 1000L;
  private static final long BASE_NANO = System.nanoTime();

  /** Microseconds since the epoch, with the resolution of nanoTime (comparable across JVMs to about a millisecond). */
  public static long nowUs() {
    return BASE_EPOCH_US + (System.nanoTime() - BASE_NANO) / 1000L;
  }

  /**
   * Event-time environment with object reuse, the job parallelism of CAPS_PARALLELISM and the watermark interval.
   * In the in-process runtime (java -cp ... Main) it also gives the mini cluster enough network memory for a wide
   * shuffle and, if CAPS_METRICS_FILE is set, the metric reporter that writes the per-operator counters.
   */
  public static StreamExecutionEnvironment environment(DisorderConfig cfg) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    if (env instanceof LocalStreamEnvironment) {
      Configuration conf = new Configuration();
      conf.setString("taskmanager.memory.network.min", "512mb");
      conf.setString("taskmanager.memory.network.max", "512mb");
      if (!cfg.metricsFile.isEmpty()) {
        conf.setString("metrics.reporters", "caps");
        conf.setString("metrics.reporter.caps.class", LateCountReporter.class.getName());
        conf.setString("metrics.reporter.caps.file", cfg.metricsFile);
      }
      env = StreamExecutionEnvironment.createLocalEnvironment(cfg.parallelism, conf);
    }
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.getConfig().setAutoWatermarkInterval(cfg.wmIntervalMs);
    env.setParallelism(cfg.parallelism);
    if (cfg.checkpointMs > 0) { // E4: checkpoints at a fixed interval, restart from the last one
      env.enableCheckpointing(cfg.checkpointMs);
      env.setRestartStrategy(RestartStrategies.fixedDelayRestart(cfg.restartAttempts, cfg.restartDelayMs));
    }
    return env;
  }

  /**
   * The assigner of every source: event time = the record's timestamp, periodic bounded-out-of-orderness watermark.
   * Same rule as Flink 1.10's BoundedOutOfOrdernessTimestampExtractor (watermark = newest timestamp - (delta + 1)),
   * but the newest timestamp is checkpointed. The watermark is therefore a function of the records seen so far and of
   * nothing else (the processing-time timer only decides when it is sampled), and a restored job continues from the
   * watermark position of the checkpoint instead of from the start. Data-driven and deterministic on replay.
   */
  public static final class Assigner<T> implements AssignerWithPeriodicWatermarks<T>, CheckpointedFunction {
    private static final long serialVersionUID = 1L;
    private final DisorderSource.TsFn<T> ts;
    private final long outOfOrderness;
    private long maxTs;
    private transient ListState<Long> state;

    Assigner(long deltaMs, DisorderSource.TsFn<T> ts) {
      this.outOfOrderness = deltaMs + 1;
      this.ts = ts;
      this.maxTs = Long.MIN_VALUE + outOfOrderness;
    }

    @Override
    public long extractTimestamp(T element, long previousElementTimestamp) {
      long t = ts.ts(element);
      if (t > maxTs)
        maxTs = t;
      return t;
    }

    @Override
    public Watermark getCurrentWatermark() {
      return new Watermark(maxTs - outOfOrderness);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      state.clear();
      state.add(maxTs);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      state = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("caps-wm-max", Types.LONG));
      if (context.isRestored())
        for (Long m : state.get())
          maxTs = Math.max(maxTs, m);
    }
  }

  /** One parallel reader per partition (CAPS_SOURCE_PARALLELISM) followed by its watermark assigner, chained. */
  public static <T> SingleOutputStreamOperator<T> source(StreamExecutionEnvironment env, DisorderConfig cfg,
      String name, String path, DisorderSource.Parser<T> parser, DisorderSource.TsFn<T> ts, Class<T> type) {
    return env
        .addSource(new DisorderSource<>(name, path, parser, ts, cfg, TypeInformation.of(type).createSerializer(env.getConfig())),
            name, TypeInformation.of(type))
        .setParallelism(cfg.sourceParallelism)
        .assignTimestampsAndWatermarks(new Assigner<>(cfg.deltaMs, ts))
        .setParallelism(cfg.sourceParallelism)
        .name("WATERMARKS-" + name);
  }

  /** The tag of the late records of a window operator (apply it with sideOutputLateData, then call lateSink). */
  public static <T> OutputTag<T> lateTag(String op, TypeInformation<T> type) {
    return new OutputTag<T>("late-" + op, type);
  }

  /**
   * Gives a window operator its late side output, if the late records are counted (CAPS_COUNT_LATE=1); otherwise the
   * window stream is returned unchanged (late records are dropped by Flink, as in the released artifact).
   */
  public static <T, K> WindowedStream<T, K, TimeWindow> lateOutput(WindowedStream<T, K, TimeWindow> window,
      OutputTag<T> tag, DisorderConfig cfg) {
    return cfg.countLate ? window.sideOutputLateData(tag) : window;
  }

  /** Counts the late records of a window operator (late_op_<op>_s<i>.csv in CAPS_TRACE_DIR, metric lateSideOutput); a no-op unless CAPS_COUNT_LATE=1. */
  public static <T> void lateSink(SingleOutputStreamOperator<?> window, OutputTag<T> tag, String op,
      DisorderConfig cfg) {
    if (cfg.countLate)
      window.getSideOutput(tag).addSink(new LateCounter<T>(op, cfg.traceDir)).name("LATE-" + op);
  }

  /**
   * The trigger of every window operator: Flink's default EventTimeTrigger, or, with CAPS_COUNT_LATE=1, the
   * MissedWindowTrigger, which fires the same way and also counts the records that came too late for a window.
   */
  public static Trigger<Object, TimeWindow> trigger(DisorderConfig cfg, Time slide) {
    if (cfg.countLate)
      return new MissedWindowTrigger(slide.toMilliseconds());
    return EventTimeTrigger.create();
  }

  /** env.execute plus the net runtime of the job (what the experiment scripts read). */
  public static JobExecutionResult execute(StreamExecutionEnvironment env, String name) throws Exception {
    JobExecutionResult r = env.execute(name);
    System.out.println("CAPS_NET_RUNTIME_MS " + r.getNetRuntime());
    return r;
  }

  /** Static types of the stream the tag will be given: the input of the window operator. */
  public static <T> OutputTag<T> lateTag(String op, DataStream<T> windowInput) {
    return lateTag(op, windowInput.getType());
  }
}
