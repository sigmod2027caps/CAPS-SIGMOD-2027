package temporalindex;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

// FNV-1a coin on logical output keys; 128-bit digests for order-independent verify groups.
// Identical keys share one decision — duplicates are all in or all out.
public final class LogicalSampler implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final String TOTAL_CONTRIBUTIONS = "total_contributions";
  public static final String HMP_VECTOR = "hmp_vector";

  /** FNV-1a 64-bit offset basis. */
  private static final long FNV_OFFSET = (long) 0xcbf29ce484222325L;
  private static final long FNV_PRIME = (long) 0x100000001b3L;
  /** Second 64-bit stream so a group digest is 128 bits. */
  private static final long FNV_OFFSET_B = FNV_OFFSET ^ 0x9e3779b97f4a7c15L;

  private transient ByteArrayOutputStream buf;
  private transient DataOutputStream out;

  public LogicalSampler() {
    ensureBuf();
  }

  private void ensureBuf() {
    if (buf == null) {
      buf = new ByteArrayOutputStream(128);
      out = new DataOutputStream(buf);
    }
  }

  public static long fnv64(byte[] data) {
    return fnv64(data, 0, data.length, FNV_OFFSET);
  }

  public static long fnv64(byte[] data, int off, int len, long offset) {
    long h = offset;
    int end = off + len;
    for (int i = off; i < end; i++) {
      h ^= (data[i] & 0xffL);
      h *= FNV_PRIME;
    }
    return h;
  }

  /** 53-bit threshold, same expected rate as the previous LCG coin. */
  public static long threshold53(double sampleProb) {
    if (sampleProb <= 0.0) {
      return 0L;
    }
    if (sampleProb >= 1.0) {
      return 1L << 53;
    }
    return (long) (sampleProb * (1L << 53));
  }

  public static boolean selected(long hash64, long threshold53) {
    if (threshold53 <= 0L) {
      return false;
    }
    if (threshold53 >= (1L << 53)) {
      return true;
    }
    // >>> 11 leaves 53 bits so the rate matches the old LCG coin — do not change one without the other.
    return (hash64 >>> 11) < threshold53;
  }

  public static boolean selected(long hash64, double sampleProb) {
    return selected(hash64, threshold53(sampleProb));
  }

  public static void writeLong(DataOutput out, long v) throws IOException {
    out.writeLong(v);
  }

  public static void writeDoubleBits(DataOutput out, double v) throws IOException {
    out.writeLong(Double.doubleToLongBits(v));
  }

  public static void writeString(DataOutput out, String s) throws IOException {
    out.writeUTF(s == null ? "" : s);
  }

  public static long hashBytes(byte[] key) {
    return fnv64(key);
  }

  public static long hashLongs(long[] values) {
    ByteArrayOutputStream buf = new ByteArrayOutputStream(8 * values.length);
    DataOutputStream out = new DataOutputStream(buf);
    try {
      for (int i = 0; i < values.length; i++) {
        out.writeLong(values[i]);
      }
      out.flush();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    return fnv64(buf.toByteArray());
  }

  public static long hashScalar(long value) {
    return hashLongs(new long[] {value});
  }

  /**
   * Order-independent 128-bit digest of a multiset of 64-bit item hashes:
   * sort, then two FNV-1a streams over the sorted big-endian longs.
   * Probabilistic (birthday bound), never exact.
   */
  public static long[] digest128(ArrayList<Long> itemHashes) {
    ArrayList<Long> sorted = new ArrayList<Long>(itemHashes);
    Collections.sort(sorted);
    ByteArrayOutputStream buf = new ByteArrayOutputStream(8 * sorted.size());
    DataOutputStream out = new DataOutputStream(buf);
    try {
      for (int i = 0; i < sorted.size(); i++) {
        out.writeLong(sorted.get(i).longValue());
      }
      out.flush();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    byte[] data = buf.toByteArray();
    return new long[] {
        fnv64(data, 0, data.length, FNV_OFFSET),
        fnv64(data, 0, data.length, FNV_OFFSET_B)
    };
  }

  public <T> long hashKey(SampleKeyFn<T> keyFn, T value) {
    ensureBuf();
    buf.reset();
    try {
      keyFn.writeKey(value, out);
      out.flush();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    return fnv64(buf.toByteArray());
  }

  /**
   * Running selected-key multiset (timing mode). Only sampled keys are
   * retained — about 1000 entries, not every output.
   */
  public static final class SelectedSet implements Serializable {
    private static final long serialVersionUID = 1L;
    private final HashMap<Long, long[]> counts = new HashMap<Long, long[]>();
    private int selected;
    private int unique;

    public void add(long hash64) {
      Long key = Long.valueOf(hash64);
      long[] acc = counts.get(key);
      if (acc == null) {
        acc = new long[1];
        counts.put(key, acc);
        unique++;
      }
      acc[0]++;
      selected++;
    }

    public int selected() {
      return selected;
    }

    public int unique() {
      return unique;
    }

    public ArrayList<long[]> rows() {
      ArrayList<Long> keys = new ArrayList<Long>(counts.keySet());
      Collections.sort(keys);
      ArrayList<long[]> rows = new ArrayList<long[]>(keys.size());
      for (int i = 0; i < keys.size(); i++) {
        long k = keys.get(i).longValue();
        rows.add(new long[] {k, counts.get(keys.get(i))[0]});
      }
      return rows;
    }
  }

  /** Per-timestamp verify accumulator: sorted-hash 128-bit digests. */
  public static final class VerifyGroup {
    public int count;
    public final ArrayList<Long> vectorHashes = new ArrayList<Long>();
    public final ArrayList<Long> payloadHashes = new ArrayList<Long>();
    public final ArrayList<Long> totalHashes = new ArrayList<Long>();

    public void add(Long vectorHash, long payloadHash, long totalHash) {
      count++;
      if (vectorHash != null) {
        vectorHashes.add(vectorHash);
      }
      payloadHashes.add(Long.valueOf(payloadHash));
      totalHashes.add(Long.valueOf(totalHash));
    }

    public long[] vectorDigest() {
      return digest128(vectorHashes);
    }

    public long[] payloadDigest() {
      return digest128(payloadHashes);
    }

    public long[] totalDigest() {
      return digest128(totalHashes);
    }
  }
}
