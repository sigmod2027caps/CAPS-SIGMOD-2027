package io.palyvos.provenance.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ExperimentSettingsMetadataVolumeDirTest {

  @Test
  public void parsesMetadataVolumeDirWhenPresent() {
    ExperimentSettings settings = ExperimentSettings.newInstance(new String[] {
        "--statisticsFolder", "/tmp",
        "--outputFile", "out",
        "--sourcesNumber", "1",
        "--metadataVolumeDir", "/tmp/volume-genealog"
    });
    assertEquals("/tmp/volume-genealog", settings.metadataVolumeDir());
  }

  @Test
  public void metadataVolumeDirDefaultsToEmpty() {
    ExperimentSettings settings = ExperimentSettings.newInstance(new String[] {
        "--statisticsFolder", "/tmp",
        "--outputFile", "out",
        "--sourcesNumber", "1"
    });
    String path = settings.metadataVolumeDir();
    assertTrue("unset --metadataVolumeDir must be null or empty",
        path == null || path.isEmpty());
  }
}
