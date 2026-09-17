package io.palyvos.provenance.usecases.taxi;

/**
 * Virtual sources of the taxi_1 dataflow (the paper's introduction figure):
 * the stream is the multiplex of Manhattan pickups (S1) and Queens pickups
 * (S2). Boroughs are approximated by disjoint bounding boxes over the
 * centers of the taxis.txt grid cells (make_taxis_txt.py: 40x25 cells over
 * the NYC box). Must match the noprov and caps taxi_1 jobs exactly.
 */
public final class TaxiBoroughs {

  public static final int MANHATTAN = 0;
  public static final int QUEENS = 1;

  private static final double LON_MIN = -74.05, LON_MAX = -73.75;
  private static final double LAT_MIN = 40.58, LAT_MAX = 40.92;
  private static final int LON_CELLS = 40, LAT_CELLS = 25;

  private TaxiBoroughs() {}

  /**
   * Virtual source of a pickup zone: {@link #MANHATTAN}, {@link #QUEENS}, or
   * -1 for zones outside both boroughs.
   */
  public static int borough(int zone) {
    double lon = LON_MIN + (zone % LON_CELLS + 0.5) * (LON_MAX - LON_MIN) / LON_CELLS;
    double lat = LAT_MIN + (zone / LON_CELLS + 0.5) * (LAT_MAX - LAT_MIN) / LAT_CELLS;
    if (lon >= -74.02 && lon < -73.92 && lat >= 40.70 && lat < 40.88) {
      return MANHATTAN;
    }
    if (lon >= -73.92 && lat < 40.80) {
      return QUEENS;
    }
    return -1;
  }
}
