package queryjmh;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.TimeValue;

public class QueryJmhOptionsTest {

  @Test
  public void optionsMatchRequiredForkWarmupAndAlwaysPreTouch() {
    Options options = QueryJmhMain.options("caps", "/tmp/fixture.ser");
    assertEquals(Integer.valueOf(1), options.getForkCount().get());
    assertEquals(Integer.valueOf(10), options.getWarmupIterations().get());
    assertEquals(TimeValue.seconds(1), options.getWarmupTime().get());
    assertEquals(Integer.valueOf(5), options.getMeasurementIterations().get());
    assertEquals(TimeValue.seconds(1), options.getMeasurementTime().get());
    assertEquals(TimeUnit.NANOSECONDS, options.getTimeUnit().get());
    assertTrue(options.getBenchModes().contains(Mode.AverageTime));
    assertEquals(Integer.valueOf(1), options.getThreads().get());
    Collection<String> jvmArgs = options.getJvmArgsAppend().get();
    assertTrue(jvmArgs.contains("-XX:+AlwaysPreTouch"));
    for (String arg : jvmArgs) {
      assertFalse(arg.toLowerCase().contains("epsilon"));
    }
    String include = options.getIncludes().get(0);
    assertTrue(include.contains("QueryBenchmark"));
  }
}
