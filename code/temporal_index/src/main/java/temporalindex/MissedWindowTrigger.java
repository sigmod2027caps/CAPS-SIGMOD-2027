package temporalindex;

import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

// EventTimeTrigger (the default trigger of event-time windows, same firing) that also counts the records that came
// too late for at least one of their windows. Flink's own late-record accounting (the side output, the
// numLateRecordsDropped metric) only sees a record that missed ALL its windows; with sliding windows a record
// that is behind the watermark still lands in the newer windows and is dropped silently from the older ones, so
// the window results already fired lack it. Counted once per record, at its last window (the one with the
// largest start; every record that is not dropped entirely has one): the record missed a window iff its first
// window, whose right boundary is lastStart + slide - 1, is already behind the operator's watermark.
// Metric: recordsMissedWindow (the counter exists once a record missed a window).
// Used by experiments/disorder: counts the records that missed a window, only with CAPS_COUNT_LATE=1.
public class MissedWindowTrigger extends Trigger<Object, TimeWindow> {

  private static final long serialVersionUID = 1L;
  private final long slide;
  private transient Counter missed;

  public MissedWindowTrigger(long slideMs) {
    this.slide = slideMs;
  }

  @Override
  public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
    long wm = ctx.getCurrentWatermark();
    if (timestamp <= wm) {
      long lastStart = timestamp - ((timestamp + slide) % slide);
      if (window.getStart() == lastStart && lastStart + slide - 1 <= wm) {
        if (missed == null)
          missed = ctx.getMetricGroup().counter("recordsMissedWindow");
        missed.inc();
      }
    }
    if (window.maxTimestamp() <= wm)
      return TriggerResult.FIRE;
    ctx.registerEventTimeTimer(window.maxTimestamp());
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
    return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
    return TriggerResult.CONTINUE;
  }

  @Override
  public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
    ctx.deleteEventTimeTimer(window.maxTimestamp());
  }

  @Override
  public boolean canMerge() {
    return true;
  }

  @Override
  public void onMerge(TimeWindow window, OnMergeContext ctx) {
    long windowMaxTimestamp = window.maxTimestamp();
    if (windowMaxTimestamp > ctx.getCurrentWatermark())
      ctx.registerEventTimeTimer(windowMaxTimestamp);
  }
}
