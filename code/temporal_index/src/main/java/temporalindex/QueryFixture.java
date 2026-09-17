package temporalindex;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashSet;

// Serialized 1,000-query fixture for cross-method replay — count is fixed, not configurable.
public final class QueryFixture implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final int REQUIRED_COUNT = 1000;
  public static final String METHOD_CAPS = "caps";
  public static final String METHOD_GREEN = "green";
  public static final String METHOD_GENEALOG = "genealog";

  private final String method;
  private final int channels;
  private final long[] ids;
  private final Object[] queries;

  private QueryFixture(String method, int channels, long[] ids, Object[] queries) {
    this.method = method;
    this.channels = channels;
    this.ids = ids;
    this.queries = queries;
  }

  public static QueryFixture of(String method, int channels, long[] ids, Object[] queries) {
    validate(method, ids, queries, true);
    return new QueryFixture(method, channels, ids.clone(), queries.clone());
  }

  static QueryFixture unchecked(String method, int channels, long[] ids, Object[] queries) {
    return new QueryFixture(method, channels, ids.clone(), queries.clone());
  }

  public String method() {
    return method;
  }

  public int channels() {
    return channels;
  }

  public long[] ids() {
    return ids.clone();
  }

  public Object[] queries() {
    return queries.clone();
  }

  public void write(String path) throws IOException {
    ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(path));
    try {
      out.writeObject(this);
    } finally {
      out.close();
    }
  }

  public static QueryFixture load(String path, String expectedMethod) throws IOException {
    ObjectInputStream in;
    try {
      in = new ObjectInputStream(new FileInputStream(path));
    } catch (IOException e) {
      throw new IllegalArgumentException("malformed fixture", e);
    }
    try {
      Object obj = in.readObject();
      if (!(obj instanceof QueryFixture)) {
        throw new IllegalArgumentException("malformed fixture");
      }
      QueryFixture fixture = (QueryFixture) obj;
      if (fixture.method == null || fixture.ids == null || fixture.queries == null) {
        throw new IllegalArgumentException("malformed fixture");
      }
      if (expectedMethod == null || !expectedMethod.equals(fixture.method)) {
        throw new IllegalArgumentException("wrong method: expected " + expectedMethod
            + " but was " + fixture.method);
      }
      validate(fixture.method, fixture.ids, fixture.queries, false);
      return fixture;
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("malformed fixture", e);
    } catch (IOException e) {
      throw new IllegalArgumentException("malformed fixture", e);
    } finally {
      in.close();
    }
  }

  private static void validate(String method, long[] ids, Object[] queries, boolean creating) {
    if (method == null || method.isEmpty()) {
      throw new IllegalArgumentException("missing method");
    }
    if (ids == null || queries == null) {
      throw new IllegalArgumentException("malformed fixture");
    }
    if (ids.length != queries.length) {
      throw new IllegalArgumentException("id/query count mismatch");
    }
    if (!creating && ids.length != REQUIRED_COUNT) {
      throw new IllegalArgumentException(
          "fixture count " + ids.length + " != " + REQUIRED_COUNT);
    }
    if (creating && ids.length != REQUIRED_COUNT) {
      throw new IllegalArgumentException(
          "fixture count " + ids.length + " != " + REQUIRED_COUNT);
    }
    HashSet<Long> seen = new HashSet<Long>();
    for (int i = 0; i < ids.length; i++) {
      if (queries[i] == null) {
        throw new IllegalArgumentException("malformed fixture");
      }
      Long boxed = Long.valueOf(ids[i]);
      if (!seen.add(boxed)) {
        throw new IllegalArgumentException("duplicate id");
      }
      // Unsigned sort order — manifest IDs are raw key hashes, not arrival indices.
      if (i > 0 && Long.compareUnsigned(ids[i - 1], ids[i]) >= 0) {
        throw new IllegalArgumentException("ids must be unsigned sorted");
      }
    }
  }
}
