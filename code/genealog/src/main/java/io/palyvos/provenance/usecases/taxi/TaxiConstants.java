package io.palyvos.provenance.usecases.taxi;

import org.apache.flink.streaming.api.windowing.time.Time;

public class TaxiConstants {

  public static final Time WINDOW_SIZE = Time.hours(1);
  public static final Time WINDOW_SLIDE = Time.minutes(10);
}
