package temporalindex;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;

// Seeded bottom-k over distinct 64-bit key hashes; Matcher replays first-occurrence capture.
// Rank = splitmix64(id XOR seed); tie-break is unsigned id order.
public final class QueryIdManifest {

  private static final String HEADER_PREFIX = "# count=";
  private static final String HEADER_SEED = " seed=";
  private static final Comparator<Long> UNSIGNED_ID = new Comparator<Long>() {
    @Override
    public int compare(Long a, Long b) {
      return Long.compareUnsigned(a.longValue(), b.longValue());
    }
  };

  private QueryIdManifest() {}

  static int compareUnsignedRankThenId(long rankA, long idA, long rankB, long idB) {
    int ranks = Long.compareUnsigned(rankA, rankB);
    if (ranks != 0) {
      return ranks;
    }
    return Long.compareUnsigned(idA, idB);
  }

  static long splitmix64(long z) {
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
    return z ^ (z >>> 31);
  }

  static long rank(long id, long seed) {
    return splitmix64(id ^ seed);
  }

  public static final class Generator {
    private final int size;
    private final long seed;
    private final PriorityQueue<long[]> worstFirst;
    private final HashSet<Long> retainedIds;

    public Generator(int size, long seed) {
      if (size < 0) {
        throw new IllegalArgumentException("size must be non-negative");
      }
      this.size = size;
      this.seed = seed;
      this.worstFirst = new PriorityQueue<long[]>(Math.max(1, size), new Comparator<long[]>() {
        @Override
        public int compare(long[] a, long[] b) {
          return compareUnsignedRankThenId(b[0], b[1], a[0], a[1]);
        }
      });
      this.retainedIds = new HashSet<Long>();
    }

    public void observe(long id) {
      if (size == 0) {
        return;
      }
      Long boxed = Long.valueOf(id);
      if (retainedIds.contains(boxed)) {
        return;
      }
      long r = rank(id, seed);
      if (worstFirst.size() < size) {
        add(r, boxed);
        return;
      }
      long[] worst = worstFirst.peek();
      // Max-heap of worst candidates — observe() is one pass, no second sort.
      if (compareUnsignedRankThenId(r, id, worst[0], worst[1]) < 0) {
        retainedIds.remove(Long.valueOf(worst[1]));
        worstFirst.poll();
        add(r, boxed);
      }
    }

    public int retained() {
      return retainedIds.size();
    }

    public void write(String path) throws IOException {
      ArrayList<Long> ids = new ArrayList<Long>(retainedIds);
      Collections.sort(ids, UNSIGNED_ID);
      BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
          new FileOutputStream(path), StandardCharsets.UTF_8));
      try {
        out.write(HEADER_PREFIX);
        out.write(Integer.toString(ids.size()));
        out.write(HEADER_SEED);
        out.write(Long.toString(seed));
        out.write('\n');
        for (int i = 0; i < ids.size(); i++) {
          out.write(Long.toString(ids.get(i).longValue()));
          out.write('\n');
        }
      } finally {
        out.close();
      }
    }

    private void add(long rank, Long id) {
      worstFirst.add(new long[] {rank, id.longValue()});
      retainedIds.add(id);
    }
  }

  public static final class Matcher {
    private final int total;
    private final HashSet<Long> pending;
    private final HashSet<Long> matchedIds;

    private Matcher(int total, HashSet<Long> pending) {
      this.total = total;
      this.pending = pending;
      this.matchedIds = new HashSet<Long>();
    }

    public static Matcher load(String path) throws IOException {
      BufferedReader in = new BufferedReader(new InputStreamReader(
          new FileInputStream(path), StandardCharsets.UTF_8));
      try {
        String header = in.readLine();
        if (header == null || !header.startsWith(HEADER_PREFIX)) {
          throw new IllegalArgumentException("missing count/seed header");
        }
        int seedAt = header.indexOf(HEADER_SEED);
        if (seedAt < 0 || header.indexOf(HEADER_SEED, seedAt + HEADER_SEED.length()) >= 0) {
          throw new IllegalArgumentException("malformed count/seed header");
        }
        String countText = header.substring(HEADER_PREFIX.length(), seedAt);
        String seedText = header.substring(seedAt + HEADER_SEED.length());
        int count = parseHeaderInt(countText, "count");
        parseHeaderLong(seedText, "seed");
        HashSet<Long> pending = new HashSet<Long>();
        String line;
        while ((line = in.readLine()) != null) {
          if (line.isEmpty() || line.indexOf(' ') >= 0 || line.indexOf('\t') >= 0) {
            throw new IllegalArgumentException("malformed id line");
          }
          long id;
          try {
            id = Long.parseLong(line);
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("malformed id line", e);
          }
          if (!line.equals(Long.toString(id))) {
            throw new IllegalArgumentException("malformed id line");
          }
          if (!pending.add(Long.valueOf(id))) {
            throw new IllegalArgumentException("duplicate id " + id);
          }
        }
        if (pending.size() != count) {
          throw new IllegalArgumentException(
              "header count " + count + " != " + pending.size() + " ids");
        }
        return new Matcher(count, pending);
      } finally {
        in.close();
      }
    }

    public boolean selectFirst(long id) {
      Long boxed = Long.valueOf(id);
      if (!pending.remove(boxed)) {
        return false;
      }
      matchedIds.add(boxed);
      return true;
    }

    public int total() {
      return total;
    }

    public int matched() {
      return matchedIds.size();
    }

    public int remaining() {
      return pending.size();
    }

    public ArrayList<Long> matchedRows() {
      ArrayList<Long> rows = new ArrayList<Long>(matchedIds);
      Collections.sort(rows, UNSIGNED_ID);
      return rows;
    }
  }

  private static int parseHeaderInt(String text, String field) {
    long value = parseHeaderLong(text, field);
    if (value < 0L || value > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("invalid header " + field);
    }
    return (int) value;
  }

  private static long parseHeaderLong(String text, String field) {
    try {
      long value = Long.parseLong(text);
      if (!text.equals(Long.toString(value))) {
        throw new IllegalArgumentException("invalid header " + field);
      }
      return value;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("invalid header " + field, e);
    }
  }
}
