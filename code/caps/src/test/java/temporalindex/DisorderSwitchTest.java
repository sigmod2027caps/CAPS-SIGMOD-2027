package temporalindex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

// CAPS_COUNT_LATE: the late-record counting of experiment E3 (MissedWindowTrigger, late side outputs) is off by default,
// so that the four methods run the same operators; with the switch on the window operators get both.
public class DisorderSwitchTest {

  private static DisorderConfig cfg(String countLate) {
    Map<String, String> env = new HashMap<>();
    if (countLate != null)
      env.put("CAPS_COUNT_LATE", countLate);
    return DisorderConfig.of(env);
  }

  @Test
  public void offByDefault() {
    assertFalse(cfg(null).countLate);
    assertFalse(cfg("0").countLate);
    assertTrue(cfg("1").countLate);
  }

  @Test
  public void defaultTriggerIsFlinksEventTimeTrigger() {
    assertEquals(EventTimeTrigger.class, Disorder.trigger(cfg(null), Time.minutes(10)).getClass());
    assertEquals(MissedWindowTrigger.class, Disorder.trigger(cfg("1"), Time.minutes(10)).getClass());
  }

  @Test
  public void jobGraphHasNoLateOperatorUnlessSwitchedOn() {
    assertFalse(operatorNames(cfg(null)).contains("LATE-W"));
    assertTrue(operatorNames(cfg("1")).contains("LATE-W"));
  }

  @Test
  public void lateOutputLeavesTheWindowStreamUntouchedWhenOff() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
    WindowedStream<Long, Long, TimeWindow> w = env.fromElements(1L).keyBy(x -> x)
        .window(TumblingEventTimeWindows.of(Time.seconds(1)));
    OutputTag<Long> tag = new OutputTag<Long>("late-W", org.apache.flink.api.common.typeinfo.Types.LONG);
    assertSame(w, Disorder.lateOutput(w, tag, cfg(null)));
  }

  // An unchecked reader does not take the checkpoint lock around its lines and position (a measurable cost), so it must
  // refuse to be snapshotted.
  @Test(expected = IllegalStateException.class)
  public void anUncheckedReaderRefusesToBeSnapshotted() throws Exception {
    new DisorderSource<Long>("S", "unused", null, null, cfg(null)).snapshotState(null);
  }

  private static String operatorNames(DisorderConfig cfg) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
    OutputTag<Long> tag = new OutputTag<Long>("late-W", org.apache.flink.api.common.typeinfo.Types.LONG);
    SingleOutputStreamOperator<Long> op = Disorder.lateOutput(env.fromElements(1L).keyBy(x -> x)
        .window(TumblingEventTimeWindows.of(Time.seconds(1)))
        .trigger(Disorder.trigger(cfg, Time.seconds(1))), tag, cfg)
        .reduce((a, b) -> a + b).name("W");
    Disorder.lateSink(op, tag, "W", cfg);
    op.addSink(new DiscardingSink<Long>());
    StreamGraph g = env.getStreamGraph();
    StringBuilder sb = new StringBuilder();
    for (StreamNode n : g.getStreamNodes())
      sb.append(n.getOperatorName()).append('|');
    return sb.toString();
  }
}
