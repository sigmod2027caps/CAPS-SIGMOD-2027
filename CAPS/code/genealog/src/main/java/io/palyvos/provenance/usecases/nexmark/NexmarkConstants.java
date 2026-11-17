package io.palyvos.provenance.usecases.nexmark;

import org.apache.flink.streaming.api.windowing.time.Time;

public class NexmarkConstants {

  public static final Time WINDOW_SIZE = Time.hours(12);
}
