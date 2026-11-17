package caps;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import tieredindex.TwoTierIndex;

/**
 * Tier 2 of one sink subtask on stable storage, as an append-only list of immutable segment files
 * seg-0, seg-1, ...: segment n holds the Tier-2 records that were migrated between checkpoint n - 1 and
 * checkpoint n, as (timestamp, per-record counts); the prefix sums are not stored, restore re-appends the
 * records. A checkpoint therefore writes only the new records, and the Flink-managed part of the checkpoint
 * holds just this list (segment lengths), not the data.
 *
 * Consistency. The checkpoint (operator state) names the segments it needs and their record counts, nothing
 * else is read. A segment written by a checkpoint that never completed, or by an execution that later
 * failed, is simply not referenced; when the restored execution writes the next segment under the same
 * number it overwrites it (the content is the same records). Segments referenced by a completed checkpoint are
 * never rewritten, because every later checkpoint appends under a higher number.
 *
 * The directory must be on a file system every TaskManager can read, exactly like the checkpoint directory
 * (it goes through Flink's FileSystem, so hdfs:// or s3:// work). Segments are not deleted while the job
 * runs; Tier 2 is append-only, so every segment stays live. A job that ends deletes nothing here, the
 * directory is the job's retained state.
 * The durable Tier 2 of the recovery experiment (E4); see TieredIndexSink for the checkpointed state.
 */
public class SegmentStore {

  /** Records per write chunk (bounds the temporary arrays). */
  static final int CHUNK = 1 << 14;

  final Path dir;
  final int channels;
  /** Records per segment, in order. */
  final List<Long> lengths = new ArrayList<>();
  /** Tier-2 records that are in segments already. */
  long flushed;
  /** Bytes written by this execution (and the ones it restored from are not counted). */
  long bytesWritten;

  /** dir: the directory (Flink FileSystem URI) of this subtask's segments; channels: M of the index. */
  public SegmentStore(String dir, int channels) {
    this.dir = new Path(dir);
    this.channels = channels;
  }

  private Path file(int n) {
    return new Path(dir, "seg-" + n);
  }

  /** Writes the Tier-2 records not yet in a segment as the next segment (nothing if there are none). */
  public void flush(TwoTierIndex index) throws IOException {
    int n2 = index.tier2Size();
    if (n2 == flushed)
      return;
    FileSystem fs = dir.getFileSystem();
    fs.mkdirs(dir);
    Path f = file(lengths.size());
    long count = n2 - flushed;
    long[] ts = new long[(int) Math.min(count, CHUNK)];
    long[] vals = new long[ts.length * channels];
    try (FSDataOutputStream raw = fs.create(f, FileSystem.WriteMode.OVERWRITE);
         DataOutputStream out = new DataOutputStream(new BufferedOutputStream(raw, 1 << 16))) {
      out.writeLong(count);
      for (long from = flushed; from < n2; from += CHUNK) {
        int to = (int) Math.min(n2, from + CHUNK);
        index.exportTier2((int) from, to, ts, vals);
        int m = to - (int) from;
        for (int i = 0; i < m; i++) {
          out.writeLong(ts[i]);
          for (int c = 0; c < channels; c++)
            out.writeLong(vals[i * channels + c]);
        }
      }
      out.flush();
      bytesWritten += 8 + count * 8L * (1 + channels);
    }
    lengths.add(count);
    flushed = n2;
  }

  /** The references of the checkpoint: the number of segments and their record counts. */
  public void write(DataOutputStream out) throws IOException {
    out.writeInt(lengths.size());
    for (long l : lengths)
      out.writeLong(l);
  }

  /** Reads the references from a checkpoint and replays the segments into the index (Tier 2 and its listener). */
  public void read(DataInputStream in, TwoTierIndex index) throws IOException {
    int segs = in.readInt();
    long[] len = new long[segs];
    for (int i = 0; i < segs; i++)
      len[i] = in.readLong();
    FileSystem fs = dir.getFileSystem();
    lengths.clear();
    flushed = 0;
    for (int sgm = 0; sgm < segs; sgm++) {
      try (FSDataInputStream raw = fs.open(file(sgm));
           DataInputStream seg = new DataInputStream(new BufferedInputStream(raw, 1 << 16))) {
        long count = seg.readLong();
        if (count != len[sgm])
          throw new IOException("segment " + file(sgm) + " has " + count + " records, the checkpoint says " + len[sgm]);
        long[] ts = new long[(int) Math.min(count, CHUNK)];
        long[] vals = new long[ts.length * channels];
        for (long done = 0; done < count; ) {
          int m = (int) Math.min(CHUNK, count - done);
          for (int i = 0; i < m; i++) {
            ts[i] = seg.readLong();
            for (int c = 0; c < channels; c++)
              vals[i * channels + c] = seg.readLong();
          }
          index.restoreTier2(ts, vals, m);
          done += m;
        }
      }
      lengths.add(len[sgm]);
      flushed += len[sgm];
    }
  }
}
