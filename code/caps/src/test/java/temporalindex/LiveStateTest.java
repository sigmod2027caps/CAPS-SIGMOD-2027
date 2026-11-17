package temporalindex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Map;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.junit.Before;
import org.junit.Test;

// The live-state accounting on small jobs whose peak is computed by hand: every add must be matched by a clear
// (the total is 0 after the stream), and the logical peak must be the hand-computed one.
// Elements are (key, ts); watermark = ts (ascending extractor); parallelism 1.
public class LiveStateTest {

  @Before
  public void reset() {
    LiveState.reset();
  }

  static DataStream<Tuple2<Integer, Long>> source(StreamExecutionEnvironment env, int keys, long step, long n) {
    Tuple2<Integer, Long>[] data = new Tuple2[(int) (keys * n)];
    int i = 0;
    for (long t = 0; t < n; t++)
      for (int k = 0; k < keys; k++)
        data[i++] = Tuple2.of(k, t * step);
    return env.fromElements(data).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Integer, Long>>() {
      @Override
      public long extractAscendingTimestamp(Tuple2<Integer, Long> e) {
        return e.f1;
      }
    });
  }

  static StreamExecutionEnvironment env() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setParallelism(1);
    return env;
  }

  /** Counts the elements of a pane; calls grow(8) per element when growing (the Green pattern). */
  public static class Count implements AggregateFunction<Tuple2<Integer, Long>, long[], Long> {
    final boolean growing;

    Count(boolean growing) {
      this.growing = growing;
    }

    @Override
    public long[] createAccumulator() {
      return new long[1];
    }

    @Override
    public long[] add(Tuple2<Integer, Long> v, long[] acc) {
      acc[0]++;
      if (growing)
        LiveState.grow(8);
      return acc;
    }

    @Override
    public Long getResult(long[] acc) {
      return acc[0];
    }

    @Override
    public long[] merge(long[] a, long[] b) {
      a[0] += b[0];
      return a;
    }
  }

  static <X> Map<String, Long> run(StreamExecutionEnvironment env, DataStream<X> out) throws Exception {
    out.addSink(new LiveStateSink<X>());
    JobExecutionResult r = env.execute();
    Map<String, Long> m = new java.util.TreeMap<>();
    for (Map.Entry<String, Object> e : r.getAllAccumulatorResults().entrySet())
      if (e.getKey().startsWith(LiveState.ACC_PREFIX))
        m.put(e.getKey().substring(LiveState.ACC_PREFIX.length()), ((Number) e.getValue()).longValue());
    return m;
  }

  /**
   * Sliding 4 s / 2 s windows, one key, an element every 1 s for 10 s, 16 B per accumulator. A pane lives from its
   * first element to its end, so two panes are alive at a time: peak 32. Six panes are created over the run.
   */
  @Test
  public void slidingAggregateOneAccumulatorPerPane() throws Exception {
    StreamExecutionEnvironment env = env();
    DataStream<Long> out = source(env, 1, 1000, 10)
        .keyBy(e -> e.f0)
        .window(SlidingEventTimeWindows.of(Time.seconds(4), Time.seconds(2)))
        .trigger(new LiveState.StateTrigger<Tuple2<Integer, Long>>(EventTimeTrigger.create(), "AGG", 0, 2000,
            LiveState.constant(16), null))
        .aggregate(new Count(false));
    Map<String, Long> m = run(env, out);
    assertEquals(32L, (long) m.get("logical_peak_total"));
    assertEquals(0L, (long) m.get("logical_end_total"));
    assertEquals(0L, (long) m.get("phys_end_total"));
    assertEquals(6L, (long) m.get("adds"));
    assertEquals(6L, (long) m.get("clears"));
    // periodic watermarks: all six panes can be live at once in wall time (96), never fewer than the lock-step 32
    assertTrue(m.get("phys_peak_total") >= 32 && m.get("phys_peak_total") <= 96);
  }

  /**
   * The Green pattern: the accumulator grows by 8 B per element. Tumbling 4 s windows, two keys, an element per key
   * every 1 s: a pane holds 4 elements = 32 B, two keys = 64 B just before the purge.
   */
  @Test
  public void growingAccumulator() throws Exception {
    StreamExecutionEnvironment env = env();
    DataStream<Long> out = source(env, 2, 1000, 12)
        .keyBy(e -> e.f0)
        .window(TumblingEventTimeWindows.of(Time.seconds(4)))
        .trigger(new LiveState.StateTrigger<Tuple2<Integer, Long>>(EventTimeTrigger.create(), "AGG", 0, 4000, null, null))
        .aggregate(new Count(true));
    Map<String, Long> m = run(env, out);
    assertEquals(64L, (long) m.get("logical_peak_total"));
    assertEquals(0L, (long) m.get("logical_end_total"));
    assertEquals(0L, (long) m.get("phys_end_total"));
    assertEquals(24L, (long) m.get("adds"));
  }

  /**
   * Window join, tumbling 4 s, one key: each side has an element per second, the join buffers every element (8 B)
   * until the window ends: 4 + 4 elements = 64 B per pane. Over the run 3 panes.
   */
  @Test
  public void joinBuffersEveryElement() throws Exception {
    StreamExecutionEnvironment env = env();
    DataStream<Tuple2<Integer, Long>> left = source(env, 1, 1000, 12);
    DataStream<Tuple2<Integer, Long>> right = source(env, 1, 1000, 12);
    DataStream<Long> out = left.join(right)
        .where(e -> e.f0)
        .equalTo(e -> e.f0)
        .window(TumblingEventTimeWindows.of(Time.seconds(4)))
        .trigger(new LiveState.StateTrigger<org.apache.flink.streaming.api.datastream.CoGroupedStreams.TaggedUnion<
            Tuple2<Integer, Long>, Tuple2<Integer, Long>>>(EventTimeTrigger.create(), "JOIN", 0, 4000, null,
            LiveState.joined(8, 8)))
        .apply((a, b) -> a.f1 + b.f1, org.apache.flink.api.common.typeinfo.Types.LONG);
    Map<String, Long> m = run(env, out);
    assertEquals(64L, (long) m.get("logical_peak_total"));
    assertEquals(0L, (long) m.get("logical_end_total"));
    assertEquals(0L, (long) m.get("phys_end_total"));
    assertEquals(24L, (long) m.get("adds"));
  }

  /**
   * Two stages: an aggregate (stage 0, 16 B per pane, tumbling 4 s) feeds an aggregate (stage 1, 16 B per pane and
   * 8 B per element). The first stage's result is stamped with the window end, so it enters the second stage at
   * the instant the first purged: at no time are both panes live. Peak = max(16, 16 + 8) = 24, not 40.
   */
  @Test
  public void stagesAreOrderedAtOneInstant() throws Exception {
    StreamExecutionEnvironment env = env();
    DataStream<Tuple2<Integer, Long>> stage0 = source(env, 1, 1000, 12)
        .keyBy(e -> e.f0)
        .window(TumblingEventTimeWindows.of(Time.seconds(4)))
        .trigger(new LiveState.StateTrigger<Tuple2<Integer, Long>>(EventTimeTrigger.create(), "A0", 0, 4000,
            LiveState.constant(16), null))
        .aggregate(new Count(false))
        .map(n -> Tuple2.of(0, n))
        .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(org.apache.flink.api.common.typeinfo.Types.INT,
            org.apache.flink.api.common.typeinfo.Types.LONG));
    DataStream<Long> out = stage0
        .keyBy(e -> e.f0)
        .window(TumblingEventTimeWindows.of(Time.seconds(4)))
        .trigger(new LiveState.StateTrigger<Tuple2<Integer, Long>>(EventTimeTrigger.create(), "A1", 1, 4000,
            LiveState.constant(16), LiveState.constant(8)))
        .aggregate(new Count(false));
    Map<String, Long> m = run(env, out);
    assertEquals(24L, (long) m.get("logical_peak_total"));
    assertEquals(0L, (long) m.get("logical_end_total"));
    assertEquals(0L, (long) m.get("phys_end_total"));
    assertEquals(0L, (long) m.get("logical_at_peak.A0"));
    assertEquals(24L, (long) m.get("logical_at_peak.A1"));
  }

  /**
   * Pinned elements (GeneaLog's pointer chain): sliding 4 s / 2 s, an element per second, 16 B per accumulator and
   * 8 B per element that the accumulators keep alive. An element is charged once, to its newest pane, and released
   * when that pane is purged. Steady state before a purge: two accumulators (32) + the elements of the two newest
   * panes' first slides (4 elements, 32) = 64.
   */
  @Test
  public void pinnedElementsAreChargedOnce() throws Exception {
    StreamExecutionEnvironment env = env();
    DataStream<Long> out = source(env, 1, 1000, 10)
        .keyBy(e -> e.f0)
        .window(SlidingEventTimeWindows.of(Time.seconds(4), Time.seconds(2)))
        .trigger(new LiveState.StateTrigger<Tuple2<Integer, Long>>(EventTimeTrigger.create(), "AGG", 0, 2000,
            LiveState.constant(16), null, LiveState.constant(8), null))
        .aggregate(new Count(false));
    Map<String, Long> m = run(env, out);
    assertEquals(64L, (long) m.get("logical_peak_total"));
    assertEquals(32L, (long) m.get("logical_peak_core"));   // the accumulators alone
    assertEquals(0L, (long) m.get("logical_end_total"));
    assertEquals(0L, (long) m.get("phys_end_total"));
    assertEquals(16L, (long) m.get("adds"));
    assertEquals(11L, (long) m.get("clears"));   // 6 panes + the 5 newest panes that own elements
  }

  /**
   * Extra bytes per element (GeneaLog's provenance set of a REMOTE tuple): tumbling 4 s, one element per second, 16 B per
   * accumulator, 8 B per element, 4 B extra per element. A pane holds 16 + 4*8 = 48 B in the core model, 48 + 4*4 = 64 with
   * the extra bytes.
   */
  @Test
  public void extraBytesAreReportedSeparately() throws Exception {
    StreamExecutionEnvironment env = env();
    DataStream<Long> out = source(env, 1, 1000, 12)
        .keyBy(e -> e.f0)
        .window(TumblingEventTimeWindows.of(Time.seconds(4)))
        .trigger(new LiveState.StateTrigger<Tuple2<Integer, Long>>(EventTimeTrigger.create(), "AGG", 0, 4000,
            LiveState.constant(16), LiveState.constant(8), null, LiveState.constant(4)))
        .aggregate(new Count(false));
    Map<String, Long> m = run(env, out);
    assertEquals(64L, (long) m.get("logical_peak_total"));
    assertEquals(48L, (long) m.get("logical_peak_core"));
    assertEquals(0L, (long) m.get("logical_end_total"));
    assertEquals(0L, (long) m.get("phys_end_total"));
    assertEquals(48L, (long) m.get("logical_at_peak.AGG"));
    assertEquals(16L, (long) m.get("logical_at_peak.AGG+extra"));
  }

  /** The trigger does not change what the window computes. */
  @Test
  public void resultsUnchanged() throws Exception {
    long[] plain = results(false);
    long[] wrapped = results(true);
    assertTrue(Arrays.equals(plain, wrapped));
    assertTrue(plain.length > 0);
  }

  static long[] results(boolean wrap) throws Exception {
    LiveState.reset();
    StreamExecutionEnvironment env = env();
    org.apache.flink.streaming.api.datastream.WindowedStream<Tuple2<Integer, Long>, Integer,
        org.apache.flink.streaming.api.windowing.windows.TimeWindow> w = source(env, 3, 500, 40)
            .keyBy(e -> e.f0)
            .window(SlidingEventTimeWindows.of(Time.seconds(4), Time.seconds(1)));
    if (wrap)
      w = w.trigger(new LiveState.StateTrigger<Tuple2<Integer, Long>>(EventTimeTrigger.create(), "AGG", 0, 1000,
          LiveState.constant(16), null));
    GOT.clear();
    w.aggregate(new Count(false)).addSink(new org.apache.flink.streaming.api.functions.sink.SinkFunction<Long>() {
      @Override
      public void invoke(Long v, Context c) {
        GOT.add(v);
      }
    });
    env.execute();
    long[] r = new long[GOT.size()];
    for (int i = 0; i < r.length; i++)
      r[i] = GOT.get(i);
    return r;
  }

  static final java.util.List<Long> GOT = java.util.Collections.synchronizedList(new java.util.ArrayList<Long>());
}
