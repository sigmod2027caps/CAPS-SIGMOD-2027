package queryjmh;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import temporalindex.QueryFixture;

public class QueryJmhSmokeTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void syntheticCapsFixtureProducesCanonicalJmhCsv() throws Exception {
    File fixture = tmp.newFile("query_fixture_caps.ser");
    File csv = tmp.newFile("jmh_query.csv");
    fixture.delete();
    csv.delete();
    SyntheticFixture.writeCaps(fixture.getAbsolutePath());
    QueryJmhMain.main(new String[] {
        "caps", fixture.getAbsolutePath(), csv.getAbsolutePath()
    });
    BufferedReader in = new BufferedReader(
        new InputStreamReader(new FileInputStream(csv), StandardCharsets.UTF_8));
    try {
      assertEquals(QueryJmhCsv.HEADER, in.readLine());
      String row = in.readLine();
      String[] parts = row.split(",");
      assertEquals(8, parts.length);
      assertEquals("caps", parts[0]);
      double score = Double.parseDouble(parts[1]);
      double error = Double.parseDouble(parts[2]);
      assertTrue(Double.isFinite(score) && score >= 0.0);
      assertTrue(Double.isFinite(error) && error >= 0.0);
      assertEquals("5", parts[3]);
      assertEquals("ns/op", parts[4]);
      QueryWorkload workload = QueryWorkload.load("caps", fixture.getAbsolutePath());
      assertEquals(Long.toString(workload.runAll()), parts[5]);
      assertEquals(Integer.toString(QueryFixture.REQUIRED_COUNT), parts[6]);
      assertEquals(Long.toString(workload.idChecksum()), parts[7]);
    } finally {
      in.close();
    }
  }
}
