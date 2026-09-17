package io.palyvos.provenance.usecases.twitter;

import org.apache.flink.streaming.api.windowing.time.Time;

public class TwitterConstants {

  public static final Time AGG_WINDOW_SIZE = Time.minutes(30);
  public static final Time AGG_WINDOW_SLIDE = Time.minutes(5);
  public static final Time JOIN_WINDOW_SIZE = Time.minutes(30);
  public static final Time JOIN_WINDOW_SLIDE = Time.minutes(5);
}
