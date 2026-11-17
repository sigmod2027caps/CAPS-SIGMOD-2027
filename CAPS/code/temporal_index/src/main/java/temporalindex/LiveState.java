package temporalindex;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams.TaggedUnion;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Live provenance state: the annotation bytes that the window operators hold at a time.
 *
 * Definition. The live state at a time is the sum, over all window operators and all their subtasks, of the
 * annotation bytes of what the operator keeps in its window state: one accumulator per (key, window pane) of a
 * window aggregate, one buffered tuple copy per pane of a window join. Records in flight (network buffers,
 * operator chains) are transient and not counted, stateless operators hold nothing. A pane is purged by
 * Trigger.clear, and everything the pane added is subtracted there, so after the stream the total is 0.
 *
 * How. Each window operator gets its trigger wrapped in StateTrigger, which delegates every decision to the original
 * trigger (the window semantics do not change) and keeps, in per-pane partitioned state, the bytes the pane added.
 * What a pane adds is given by the method, never measured with the heap:
 * onCreate     bytes when the first element of a pane arrives (CAPS: the M-counter accumulator, GeneaLog: the
 * two pointers of the aggregate strategy),
 * perElement   bytes per element and pane (window join: the buffered copy; sliding windows buffer one per pane),
 * grow(bytes)  bytes the accumulator's annotation grew by, called from inside the aggregate function (Green),
 * pinned, extra  bytes a method holds beyond that model (GeneaLog: the tuples its aggregate chains keep alive, the
 * provenance sets of its tuples after a network exchange), under the operator names "&lt;op&gt;+pinned" and
 * "&lt;op&gt;+extra": logical_peak_core leaves them out, logical_peak_total has them.
 *
 * Two peaks. (1) Physical: JVM-wide registry, one AtomicLong per (operator, subtask) and one global total; every
 * update goes to both, and the peak is kept by a CAS loop, so it is the exact maximum of the real interleaving of the
 * updates. It depends on how far the operator threads run apart (a file source races ahead of the other one).
 * (2) Logical, the one we report: the same events ordered by event time instead of wall time, as if all the
 * sources advanced in lock step, which is what a live stream does. It is a function of the data only, so it is
 * identical in repeated runs. Between two window ends the state only grows, so the maximum is at a window end,
 * just before the purges: events are kept in buckets k = ceil((ts+1)/grain) (grain = slide), and inside a bucket
 * ordered by phase 2*stage (adds of window stage s: operators fed by the sources are stage 0, operators fed by the
 * window operators of stage s-1 are stage s) and 2*stage+1 (purges of stage s). The records of stage s-1 that a
 * window emits at its end are stamped with that end, so at one instant: adds of stage 0, purges of stage 0, adds
 * of stage 1, purges of stage 1, in this order.
 */
public final class LiveState {

  /** Annotation bytes of one element (or accumulator). */
  public interface Sizer<T> extends Serializable {
    long bytes(T element);
  }

  public static final String MODE = "live_memory";
  public static final String CSV_FILENAME = "live_state.csv";
  public static final String ACC_PREFIX = "live.";
  static final int MAX_STAGE = 3;
  /** Suffix of the operator name under which the bytes pinned by pointers are reported (see trigger()). */
  public static final String PINNED = "+pinned";
  /** Suffix of the operator name under which the extra bytes per element are reported. */
  public static final String EXTRA = "+extra";

  static final ConcurrentHashMap<String, Local> LOCALS = new ConcurrentHashMap<>();
  static final AtomicLong TOTAL = new AtomicLong();
  static final AtomicLong PEAK = new AtomicLong();
  static final AtomicInteger CLOSED_SINKS = new AtomicInteger();
  static final ThreadLocal<long[]> PENDING = new ThreadLocal<long[]>() {
    @Override
    protected long[] initialValue() {
      return new long[1];
    }
  };

  /** Read where the job graph is built (the client): CAPS_LIVE_STATE=1 wraps the triggers of the window operators. */
  public static boolean on() {
    return "1".equals(System.getenv("CAPS_LIVE_STATE"));
  }

  /** Mode argument of the dataflows: live_memory <outputDir>. */
  public static boolean enabled(String[] args, int offset) {
    return args != null && args.length > offset && MODE.equals(args[offset]);
  }

  /** The same number of bytes for every element. */
  public static Sizer<Object> constant(final long bytes) {
    return new Sizer<Object>() {
      private static final long serialVersionUID = 1L;
      @Override
      public long bytes(Object element) {
        return bytes;
      }
    };
  }

  /** Size of a window-join element when the left and the right input have a constant size each. */
  public static Sizer<Object> joined(final long left, final long right) {
    return new Sizer<Object>() {
      private static final long serialVersionUID = 1L;
      @Override
      public long bytes(Object e) {
        return ((TaggedUnion<?, ?>) e).isOne() ? left : right;
      }
    };
  }

  /** Size of a window-join element: the left or the right input's size. */
  public static <A, B> Sizer<TaggedUnion<A, B>> join(final Sizer<A> left, final Sizer<B> right) {
    return new Sizer<TaggedUnion<A, B>>() {
      private static final long serialVersionUID = 1L;
      @Override
      public long bytes(TaggedUnion<A, B> e) {
        return e.isOne() ? left.bytes(e.getOne()) : right.bytes(e.getTwo());
      }
    };
  }

  /**
   * The trigger to give to a window operator: the original trigger, observed, when the live state is on; the
   * original trigger itself otherwise. {@code slide}: the grain of the logical clock (the window ends are multiples of it).
   */
  public static <T> Trigger<T, TimeWindow> trigger(Trigger<? super T, TimeWindow> base, String op, int stage,
      Time slide, Sizer<? super T> onCreate, Sizer<? super T> perElement) {
    return trigger(base, op, stage, slide, onCreate, perElement, null, null);
  }

  /**
   * As above, plus two kinds of bytes that a method holds BEYOND its annotation model, reported under their own operator
   * names so that the core model (without them) and the full state (with them) are both available:
   * {@code pinned}: bytes of an element that the accumulators keep alive by a pointer without copying it (GeneaLog's pointer
   * chain), charged once, to the newest pane that contains the element (the last of its panes to be purged);
   * {@code extra}: bytes per element and pane on top of {@code perElement} (the provenance set that a GeneaLog tuple carries
   * after a network exchange).
   */
  @SuppressWarnings("unchecked")
  public static <T> Trigger<T, TimeWindow> trigger(Trigger<? super T, TimeWindow> base, String op, int stage,
      Time slide, Sizer<? super T> onCreate, Sizer<? super T> perElement, Sizer<? super T> pinned,
      Sizer<? super T> extra) {
    if (!on())
      return (Trigger<T, TimeWindow>) base;
    return new StateTrigger<T>(base, op, stage, slide.toMilliseconds(), onCreate, perElement, pinned, extra);
  }

  /** Called inside an aggregate function when its accumulator's annotation grew by the given bytes. */
  public static void grow(long bytes) {
    PENDING.get()[0] += bytes;
  }

  static long takePending() {
    long[] p = PENDING.get();
    long v = p[0];
    p[0] = 0;
    return v;
  }

  // ---------------------------------------------------------------------------------------------------------
  // The state of one (operator, subtask).
  static final class Local {
    final String op;
    final int subtask;
    final int stage;
    final long grain;
    long bytes;                       // live now (this thread writes it, the registry reads it after the end)
    long peak;                        // own maximum
    long elements;                    // onElement calls (an element in k panes counts k), also with 0 bytes
    long adds;
    long clears;
    final HashMap<Long, long[]> buckets = new HashMap<>();   // k -> {added, purged}
    long lastKey = Long.MIN_VALUE;
    long[] lastRow;

    Local(String op, int subtask, int stage, long grain) {
      this.op = op;
      this.subtask = subtask;
      this.stage = stage;
      this.grain = grain;
    }

    long[] row(long k) {
      if (k != lastKey) {
        long[] r = buckets.get(k);
        if (r == null) {
          r = new long[2];
          buckets.put(k, r);
        }
        lastKey = k;
        lastRow = r;
      }
      return lastRow;
    }

    synchronized void touch() {
      elements++;
    }

    synchronized void add(long delta, long ts) {
      bytes += delta;
      if (bytes > peak)
        peak = bytes;
      adds++;
      row(Math.floorDiv(ts, grain) + 1)[0] += delta;
      long t = TOTAL.addAndGet(delta);
      long p = PEAK.get();
      while (t > p && !PEAK.compareAndSet(p, t))
        p = PEAK.get();
    }

    synchronized void clear(long delta, long windowEnd) {
      if (windowEnd % grain != 0)
        throw new IllegalStateException(op + ": window end " + windowEnd + " is not a multiple of the slide " + grain);
      bytes -= delta;
      clears++;
      row(windowEnd / grain)[1] += delta;
      TOTAL.addAndGet(-delta);
    }
  }

  static Local local(String op, int subtask, int stage, long grain) {
    if (stage < 0 || stage > MAX_STAGE)
      throw new IllegalArgumentException("stage " + stage);
    String key = op + "#" + subtask;
    Local l = LOCALS.get(key);
    if (l == null) {
      l = new Local(op, subtask, stage, grain);
      Local prev = LOCALS.putIfAbsent(key, l);
      if (prev != null)
        l = prev;
    }
    return l;
  }

  // ---------------------------------------------------------------------------------------------------------
  /** The trigger wrapper. Same decisions as the original; only counts. */
  public static final class StateTrigger<T> extends Trigger<T, TimeWindow> {
    private static final long serialVersionUID = 1L;
    final Trigger<? super T, TimeWindow> inner;
    final String op;
    final int stage;
    final long grain;
    final Sizer<? super T> onCreate;
    final Sizer<? super T> perElement;
    final Sizer<? super T> pinned;
    final Sizer<? super T> extra;
    // Bytes the pane has added, per kind: the core model, the pinned elements, the extra bytes per element.
    final ValueStateDescriptor<Long> bytesDesc = new ValueStateDescriptor<>("live-state-bytes", Long.class);
    final ValueStateDescriptor<Long> pinnedDesc = new ValueStateDescriptor<>("live-state-pinned", Long.class);
    final ValueStateDescriptor<Long> extraDesc = new ValueStateDescriptor<>("live-state-extra", Long.class);
    transient Local local;
    transient Local pinnedLocal;
    transient Local extraLocal;

    StateTrigger(Trigger<? super T, TimeWindow> inner, String op, int stage, long grain, Sizer<? super T> onCreate,
        Sizer<? super T> perElement) {
      this(inner, op, stage, grain, onCreate, perElement, null, null);
    }

    StateTrigger(Trigger<? super T, TimeWindow> inner, String op, int stage, long grain, Sizer<? super T> onCreate,
        Sizer<? super T> perElement, Sizer<? super T> pinned, Sizer<? super T> extra) {
      this.inner = inner;
      this.op = op;
      this.stage = stage;
      this.grain = grain;
      this.onCreate = onCreate;
      this.perElement = perElement;
      this.pinned = pinned;
      this.extra = extra;
    }

    int subtask(TriggerContext ctx) {
      String idx = ctx.getMetricGroup().getAllVariables().get("<subtask_index>");
      return idx == null ? 0 : Integer.parseInt(idx);
    }

    Local local(TriggerContext ctx) {
      if (local == null)
        local = LiveState.local(op, subtask(ctx), stage, grain);
      return local;
    }

    Local pinnedLocal(TriggerContext ctx) {
      if (pinnedLocal == null)
        pinnedLocal = LiveState.local(op + PINNED, subtask(ctx), stage, grain);
      return pinnedLocal;
    }

    Local extraLocal(TriggerContext ctx) {
      if (extraLocal == null)
        extraLocal = LiveState.local(op + EXTRA, subtask(ctx), stage, grain);
      return extraLocal;
    }

    /** Adds bytes to the pane's counter in the given state and to the registry. */
    void charge(ValueState<Long> state, Local l, long bytes, long ts) throws Exception {
      Long cur = state.value();
      l.add(bytes, ts);
      state.update((cur == null ? 0L : cur) + bytes);
    }

    @Override
    public TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx)
        throws Exception {
      ValueState<Long> state = ctx.getPartitionedState(bytesDesc);
      Long cur = state.value();
      local(ctx).touch();
      long delta = takePending();
      if (cur == null && onCreate != null)
        delta += onCreate.bytes(element);
      if (perElement != null)
        delta += perElement.bytes(element);
      if (cur == null || delta != 0) {
        if (delta != 0)
          local(ctx).add(delta, timestamp);
        state.update((cur == null ? 0L : cur) + delta);
      }
      if (extra != null) {
        long bytes = extra.bytes(element);
        if (bytes != 0)
          charge(ctx.getPartitionedState(extraDesc), extraLocal(ctx), bytes, timestamp);
      }
      // The first pane of an element in assigner order is its newest one, the one whose first slide contains it.
      if (pinned != null && timestamp < window.getStart() + grain) {
        long bytes = pinned.bytes(element);
        if (bytes != 0)
          charge(ctx.getPartitionedState(pinnedDesc), pinnedLocal(ctx), bytes, timestamp);
      }
      return inner.onElement(element, timestamp, window, ctx);
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
      return inner.onProcessingTime(time, window, ctx);
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
      return inner.onEventTime(time, window, ctx);
    }

    /** Subtracts what the pane added in the given state and clears it. */
    void release(ValueState<Long> state, Local l, TimeWindow window) throws Exception {
      Long cur = state.value();
      if (cur != null) {
        if (cur != 0)
          l.clear(cur, window.getEnd());
        state.clear();
      }
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
      release(ctx.getPartitionedState(bytesDesc), local(ctx), window);
      if (extra != null)
        release(ctx.getPartitionedState(extraDesc), extraLocal(ctx), window);
      if (pinned != null)
        release(ctx.getPartitionedState(pinnedDesc), pinnedLocal(ctx), window);
      inner.clear(window, ctx);
    }

    @Override
    public boolean canMerge() {
      return false;
    }
  }

  // ---------------------------------------------------------------------------------------------------------
  // Report.

  /** Registers a closing sink subtask; true for the last one, which publishes the numbers. */
  public static boolean sinkClosed(int sinkParallelism) {
    return CLOSED_SINKS.incrementAndGet() == sinkParallelism;
  }

  /** Clears the registry (tests, and a second job in the same JVM). */
  public static void reset() {
    LOCALS.clear();
    TOTAL.set(0);
    PEAK.set(0);
    CLOSED_SINKS.set(0);
    PENDING.get()[0] = 0;
  }

  /** One event of the logical series. */
  static final class Event implements Comparable<Event> {
    final long bucket;
    final int phase;
    final String op;
    final long delta;

    Event(long bucket, int phase, String op, long delta) {
      this.bucket = bucket;
      this.phase = phase;
      this.op = op;
      this.delta = delta;
    }

    @Override
    public int compareTo(Event o) {
      return bucket != o.bucket ? Long.compare(bucket, o.bucket) : Integer.compare(phase, o.phase);
    }
  }

  /**
   * All numbers of the run (name -> value), after the stream ended.
   *   phys_peak_total          maximum of the real interleaving of all updates
   *   phys_end_total           live bytes at the end (0 if every add was cleared)
   *   phys_peak.<op>.s<i>      own maximum of one operator subtask
   *   logical_peak_total       maximum of the event-time-ordered series (the reported peak)
   *   logical_peak_core        the same without the "+pinned" and "+extra" operators (the core model of a method)
   *   logical_end_total        its final value (0)
   *   logical_peak_ts          event time of the peak: the window end (ms) of its bucket
   *   logical_at_peak.<op>     bytes of operator op (all subtasks) at the logical peak
   *   elements                 number of (element, pane) arrivals observed, also those that add 0 bytes
   *   adds, clears             number of additions of bytes and of purges of bytes
   */
  public static TreeMap<String, Long> report() {
    TreeMap<String, Long> out = new TreeMap<>();
    List<Event> events = new ArrayList<>();
    long elements = 0, adds = 0, clears = 0, grain = 1;
    TreeMap<String, Integer> ops = new TreeMap<>();
    for (Local l : LOCALS.values()) {
      synchronized (l) {
        out.put("phys_peak." + l.op + ".s" + l.subtask, l.peak);
        elements += l.elements;
        adds += l.adds;
        clears += l.clears;
        grain = l.grain;
        ops.put(l.op, l.stage);
        for (Map.Entry<Long, long[]> e : l.buckets.entrySet()) {
          events.add(new Event(e.getKey(), 2 * l.stage, l.op, e.getValue()[0]));
          events.add(new Event(e.getKey(), 2 * l.stage + 1, l.op, -e.getValue()[1]));
        }
      }
    }
    Collections.sort(events);
    // Two series over the same events: everything, and the core model (without the "+" operators).
    HashMap<String, Long> atPeak = new HashMap<>();
    long[] full = replay(events, false, atPeak);
    long[] core = replay(events, true, new HashMap<String, Long>());
    out.put("phys_peak_total", PEAK.get());
    out.put("phys_end_total", TOTAL.get());
    out.put("logical_peak_total", full[0]);
    out.put("logical_end_total", full[1]);
    out.put("logical_peak_ts", full[2] * grain);
    out.put("logical_peak_core", core[0]);
    for (String op : ops.keySet())
      out.put("logical_at_peak." + op, atPeak.getOrDefault(op, 0L));
    out.put("elements", elements);
    out.put("adds", adds);
    out.put("clears", clears);
    return out;
  }

  /** Running sum of the sorted events (skipping the "+" operators if coreOnly): {peak, final total, bucket of the peak}. */
  static long[] replay(List<Event> sorted, boolean coreOnly, HashMap<String, Long> atPeak) {
    HashMap<String, Long> cur = new HashMap<>();
    long total = 0, peak = 0, peakBucket = 0;
    for (Event e : sorted) {
      if (coreOnly && e.op.indexOf('+') >= 0)
        continue;
      cur.merge(e.op, e.delta, Long::sum);
      total += e.delta;
      if (total > peak) {
        peak = total;
        peakBucket = e.bucket;
        atPeak.clear();
        atPeak.putAll(cur);
      }
    }
    return new long[] {peak, total, peakBucket};
  }

  /** Client side, after the job: writes name,value of every live.* accumulator to <dir>/live_state.csv. */
  public static void writeIfEnabled(JobExecutionResult result, String[] args, int offset, String dataflow,
      String method) throws IOException {
    if (!enabled(args, offset))
      return;
    if (args.length <= offset + 1)
      throw new IllegalArgumentException(MODE + " requires an output directory");
    write(result.getAllAccumulatorResults(), new File(args[offset + 1]), dataflow, method);
  }

  public static void write(Map<String, Object> accumulators, File dir, String dataflow, String method)
      throws IOException {
    TreeMap<String, Long> values = new TreeMap<>();
    for (Map.Entry<String, Object> e : accumulators.entrySet())
      if (e.getKey().startsWith(ACC_PREFIX))
        values.put(e.getKey().substring(ACC_PREFIX.length()), ((Number) e.getValue()).longValue());
    if (!values.containsKey("logical_peak_total"))
      throw new IllegalStateException("no live state was published (is CAPS_LIVE_STATE=1 set for the client?)");
    dir.mkdirs();
    try (PrintWriter pw = new PrintWriter(new FileWriter(new File(dir, CSV_FILENAME)))) {
      pw.println("dataflow,method,name,value");
      for (Map.Entry<String, Long> e : values.entrySet())
        pw.println(dataflow + "," + method + "," + e.getKey() + "," + e.getValue());
    }
  }
}
