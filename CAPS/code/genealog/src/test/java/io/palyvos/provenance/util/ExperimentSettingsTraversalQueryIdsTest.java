package io.palyvos.provenance.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ExperimentSettingsTraversalQueryIdsTest {

  @Test
  public void parsesTraversalQueryIdsWhenPresent() {
    ExperimentSettings settings = ExperimentSettings.newInstance(new String[] {
        "--statisticsFolder", "/tmp",
        "--outputFile", "out",
        "--sourcesNumber", "1",
        "--traversalQueryIds", "/tmp/shared-ids.txt"
    });
    assertEquals("/tmp/shared-ids.txt", settings.traversalQueryIds());
  }

  @Test
  public void traversalQueryIdsDefaultsToEmpty() {
    ExperimentSettings settings = ExperimentSettings.newInstance(new String[] {
        "--statisticsFolder", "/tmp",
        "--outputFile", "out",
        "--sourcesNumber", "1"
    });
    String path = settings.traversalQueryIds();
    assertTrue("unset --traversalQueryIds must be null or empty",
        path == null || path.isEmpty());
  }
}
