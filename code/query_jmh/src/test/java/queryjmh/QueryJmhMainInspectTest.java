package queryjmh;

import static org.junit.Assert.assertEquals;

import caps.HowMuch;
import java.io.File;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import temporalindex.QueryFixture;

public class QueryJmhMainInspectTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void inspectComputesChecksumAndIdDigestOutsideMeasurement() throws Exception {
    long[] ids = new long[QueryFixture.REQUIRED_COUNT];
    Object[] queries = new Object[QueryFixture.REQUIRED_COUNT];
    long expectedChecksum = 0L;
    long expectedId = 0L;
    for (int i = 0; i < ids.length; i++) {
      ids[i] = i;
      HowMuch hm = HowMuch.of(i + 3L, 1L);
      queries[i] = hm;
      expectedChecksum += hm.total();
      expectedId += ids[i];
    }
    File path = tmp.newFile("caps.ser");
    QueryFixture.of(QueryFixture.METHOD_CAPS, 2, ids, queries).write(path.getAbsolutePath());
    QueryJmhMain.FixtureStats stats =
        QueryJmhMain.inspect(QueryFixture.METHOD_CAPS, path.getAbsolutePath());
    assertEquals(expectedChecksum, stats.checksum);
    assertEquals(QueryFixture.REQUIRED_COUNT, stats.fixtureCount);
    assertEquals(expectedId, stats.idChecksum);
  }
}
