package caps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

// Snapshot and restore of the index sink (E4): a sink restored from a checkpoint and fed the
// records that followed it must end with exactly the index (Tier 2 digest, totals, every benchmark answer, the
// statistics) of a sink that ran without interruption; feeding it records it had already counted must not.
public class TieredIndexSinkRecoveryTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  static final int CHANNELS = 3;

  /** One step of the stream: the record, then the watermark that lags it (as in TieredIndexSinkTest). */
  static void feed(IndexingSink<TieredIndexSinkTest.Rec> sink, List<TieredIndexSinkTest.Rec> recs, int from, int to,
                   long[] clock) throws Exception {
    for (int i = from; i < to; i++) {
      TieredIndexSinkTest.Rec r = recs.get(i);
      sink.invoke(r, null);
      clock[0] = Math.max(clock[0], r.ts);
      sink.accept(new Watermark(clock[0] - 21));
    }
  }

  static IndexingSink<TieredIndexSinkTest.Rec> sink(File dir) throws Exception {
    IndexingSink<TieredIndexSinkTest.Rec> s = new IndexingSink<>(TieredIndexSinkTest.TS, TieredIndexSinkTest.HM,
        CHANNELS, dir.getAbsolutePath(), 300, 700);
    s.expectedOutputs = 6000;
    s.digestOn = true;
    s.segmentDir = new File(dir, "segments").getAbsolutePath();
    return s;
  }

  /** Runs the end of the stream and returns the files that describe the final index. */
  static List<String> finish(IndexingSink<TieredIndexSinkTest.Rec> sink, File dir) throws Exception {
    sink.accept(new Watermark(Long.MAX_VALUE));
    sink.close();
    List<String> out = new ArrayList<>();
    out.addAll(Files.readAllLines(new File(dir, "query_answers.csv").toPath()));
    out.addAll(Files.readAllLines(new File(dir, "index_digest.csv").toPath()));
    // the counters of the shard: records, indexed, late, migrated, tier-1 peak, watermarks
    String[] f = Files.readAllLines(new File(dir, "tiered_stats.csv").toPath()).get(1).split(",");
    out.add(f[3] + "," + f[4] + "," + f[5] + "," + f[6] + "," + f[7] + "," + f[13] + "," + f[15] + "," + f[16]);
    return out;
  }

  /** Reference run, and the same stream interrupted by a snapshot at `cut` records and a restore. */
  private List<List<String>> runBoth(List<TieredIndexSinkTest.Rec> recs, int cut, boolean replayFromCut)
      throws Exception {
    File refDir = tmp.newFolder();
    File aDir = tmp.newFolder();
    File bDir = tmp.newFolder();
    IndexingSink<TieredIndexSinkTest.Rec> ref = sink(refDir);
    ref.open(new Configuration());
    feed(ref, recs, 0, recs.size(), new long[] {Long.MIN_VALUE});
    List<String> expected = finish(ref, refDir);

    // execution 1: runs to the cut, snapshots, runs on (and "fails" before the end: nothing finishes)
    IndexingSink<TieredIndexSinkTest.Rec> a = TieredIndexSinkTest.copy(sink(aDir));
    a.open(new Configuration());
    a.inTask = true; // as in a Flink task: a close() without the end-of-input watermark is a teardown
    long[] clock = {Long.MIN_VALUE};
    feed(a, recs, 0, cut / 2, clock);
    a.snapshotBytes(); // an earlier checkpoint: its segment is part of every later one
    feed(a, recs, cut / 2, cut, clock);
    long[] clockAtCut = clock.clone();
    byte[] state = a.snapshotBytes();
    feed(a, recs, cut, cut + 700, clock); // records after the cut, lost with the failure
    a.close(); // torn down without the end-of-input watermark: must not run the final queries
    assertFalse(new File(aDir, "query_answers.csv").exists());

    // execution 2: restored, replays from the cut
    IndexingSink<TieredIndexSinkTest.Rec> b = TieredIndexSinkTest.copy(sink(bDir));
    b.segmentDir = a.segmentDir; // the checkpoint names segments of the first execution
    b.open(new Configuration());
    b.inTask = true;
    b.restoreFrom(state);
    int from = replayFromCut ? cut : cut - 500; // the negative control re-feeds 500 records it already counted
    feed(b, recs, from, recs.size(), replayFromCut ? clockAtCut : new long[] {clockAtCut[0]});
    List<String> restored = finish(b, bDir);
    List<List<String>> r = new ArrayList<>();
    r.add(expected);
    r.add(restored);
    return r;
  }

  private List<TieredIndexSinkTest.Rec> records() {
    return TieredIndexSinkTest.stream(7, 6000, CHANNELS, 5000);
  }

  @Test
  public void restoredSinkEndsWithTheIndexOfTheUninterruptedRun() throws Exception {
    List<List<String>> r = runBoth(records(), 3000, true);
    assertEquals(r.get(0), r.get(1));
    assertTrue(r.get(0).size() > 600); // 3 x 300 answers + digest + counters
  }

  @Test
  public void snapshotWithRecordsStillInTier1() throws Exception {
    // the watermark lags the newest record by 21 ms, so Tier 1 is never empty between steps
    List<List<String>> r = runBoth(records(), 1234, true);
    assertEquals(r.get(0), r.get(1));
  }

  @Test
  public void recordsCountedTwiceChangeTheIndex() throws Exception {
    List<List<String>> r = runBoth(records(), 3000, false);
    // 500 records contributed twice: the digest and the totals differ (the check would catch a missing dedup)
    assertNotEquals(r.get(0), r.get(1));
  }

  @Test
  public void aCheckpointWritesOnlyTheNewRecords() throws Exception {
    File dir = tmp.newFolder();
    File seg = new File(dir, "segments/index/s0");
    IndexingSink<TieredIndexSinkTest.Rec> s = sink(dir);
    s.open(new Configuration());
    List<TieredIndexSinkTest.Rec> recs = records();
    long[] clock = {Long.MIN_VALUE};
    feed(s, recs, 0, 2000, clock);
    s.snapshotBytes();
    long first = new File(seg, "seg-0").length();
    feed(s, recs, 2000, 2100, clock);
    s.snapshotBytes();
    long second = new File(seg, "seg-1").length();
    assertTrue(first > 0 && second > 0);
    assertTrue("second segment is small: " + second + " vs " + first, second < first / 5);
    assertEquals(s.shard.index.tier2Size(), (first - 8) / 8 / (1 + CHANNELS) + (second - 8) / 8 / (1 + CHANNELS));
    s.snapshotBytes(); // nothing new in Tier 2: no new file
    assertFalse(new File(seg, "seg-2").exists());
    assertEquals(3, s.snapshots);
  }

  @Test
  public void lateCountAndStatisticsAreRestored() throws Exception {
    File dir = tmp.newFolder();
    IndexingSink<TieredIndexSinkTest.Rec> s = sink(dir);
    s.open(new Configuration());
    List<TieredIndexSinkTest.Rec> recs = records();
    long[] clock = {Long.MIN_VALUE};
    feed(s, recs, 0, 1000, clock);
    s.invoke(new TieredIndexSinkTest.Rec(10, 1, 1, 1), null); // late
    s.invoke(new TieredIndexSinkTest.Rec(11, 1, 1, 1), null); // late
    byte[] state = s.snapshotBytes();
    File dir2 = tmp.newFolder();
    IndexingSink<TieredIndexSinkTest.Rec> t = sink(dir2);
    t.segmentDir = s.segmentDir;
    t.open(new Configuration());
    t.restoreFrom(state);
    assertEquals(2, t.shard.index.lateCount);
    assertEquals(s.shard.records, t.shard.records);
    assertEquals(s.shard.watermarks, t.shard.watermarks);
    assertEquals(s.shard.index.watermark(), t.shard.index.watermark());
    assertEquals(s.shard.index.tier1Size(), t.shard.index.tier1Size());
    assertEquals(s.shard.index.tier2Size(), t.shard.index.tier2Size());
    assertEquals(s.shard.index.tier2Digest(), t.shard.index.tier2Digest());
    assertEquals(s.shard.minTs, t.shard.minTs);
    assertEquals(s.shard.maxTs, t.shard.maxTs);
    assertEquals(s.shard.provisional.size(), t.shard.provisional.size());
    assertEquals(1, t.restores);
    // the scan baseline was rebuilt from the segments
    assertEquals(s.shard.array.records(), t.shard.array.records());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void sinksWithUncoveredReplicasRefuseToCheckpoint() throws Exception {
    File dir = tmp.newFolder();
    ExpirySink<TieredIndexSinkTest.Rec> e = new ExpirySink<>(TieredIndexSinkTest.TS, TieredIndexSinkTest.HM, 1,
        dir.getAbsolutePath(), 1000, 100);
    e.open(new Configuration());
    e.snapshotBytes();
  }
}
