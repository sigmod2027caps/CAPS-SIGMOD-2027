package queryjmh;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class QueryJmhCsvTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void writesCanonicalHeaderAndRow() throws Exception {
    File out = tmp.newFile("result.csv");
    QueryJmhCsv.writeCanonical(out, "caps", 12.5, 0.25, 5L, "ns/op", 99L, 1000, 42L);
    List<String> lines = readLines(out);
    assertEquals(2, lines.size());
    assertEquals("method,score_ns,error_ns,samples,unit,checksum,fixture_count,id_checksum",
        lines.get(0));
    assertEquals("caps,12.5,0.25,5,ns/op,99,1000,42", lines.get(1));
  }

  @Test
  public void rejectsNonNanosecondUnit() throws Exception {
    File out = tmp.newFile("unit.csv");
    try {
      QueryJmhCsv.writeCanonical(out, "green", 1.0, 0.1, 5L, "us/op", 1L, 1000, 1L);
      fail("expected unit rejection");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("ns/op"));
    }
  }

  @Test
  public void rejectsSampleCountOtherThanFive() throws Exception {
    File out = tmp.newFile("samples.csv");
    try {
      QueryJmhCsv.writeCanonical(out, "genealog", 1.0, 0.1, 4L, "ns/op", 1L, 1000, 1L);
      fail("expected sample-count rejection");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("5"));
    }
  }

  @Test
  public void rejectsFixtureCountOtherThanRequired() throws Exception {
    File out = tmp.newFile("count.csv");
    try {
      QueryJmhCsv.writeCanonical(out, "caps", 1.0, 0.1, 5L, "ns/op", 1L, 999, 1L);
      fail("expected fixture-count rejection");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("1000"));
    }
  }

  private static List<String> readLines(File path) throws Exception {
    ArrayList<String> lines = new ArrayList<String>();
    BufferedReader in = new BufferedReader(
        new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8));
    try {
      String line;
      while ((line = in.readLine()) != null) {
        lines.add(line);
      }
    } finally {
      in.close();
    }
    return lines;
  }
}
