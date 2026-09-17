package queryjmh;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import temporalindex.QueryFixture;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

public class QueryBenchmarkAnnotationsTest {

  @Test
  public void classHasRequiredJmhAnnotations() {
    Class<?> type = QueryBenchmark.class;
    BenchmarkMode mode = type.getAnnotation(BenchmarkMode.class);
    assertTrue(mode != null);
    assertArrayEquals(new Mode[] {Mode.AverageTime}, mode.value());
    assertEquals(TimeUnit.NANOSECONDS, type.getAnnotation(OutputTimeUnit.class).value());
    Warmup warmup = type.getAnnotation(Warmup.class);
    assertEquals(10, warmup.iterations());
    assertEquals(1, warmup.time());
    assertEquals(TimeUnit.SECONDS, warmup.timeUnit());
    Measurement measurement = type.getAnnotation(Measurement.class);
    assertEquals(5, measurement.iterations());
    assertEquals(1, measurement.time());
    assertEquals(TimeUnit.SECONDS, measurement.timeUnit());
    Fork fork = type.getAnnotation(Fork.class);
    assertEquals(1, fork.value());
    assertTrue(Arrays.asList(fork.jvmArgsAppend()).contains("-XX:+AlwaysPreTouch"));
    for (String arg : fork.jvmArgsAppend()) {
      assertFalse(arg.toLowerCase().contains("epsilon"));
    }
    assertEquals(Scope.Benchmark, type.getAnnotation(State.class).value());
    assertEquals(1, type.getAnnotation(Threads.class).value());
  }

  @Test
  public void methodParamIncludesCapsGreenGenealog() throws Exception {
    Field field = QueryBenchmark.class.getField("method");
    Param param = field.getAnnotation(Param.class);
    assertTrue(param != null);
    assertTrue(Arrays.asList(param.value()).contains("caps"));
    assertTrue(Arrays.asList(param.value()).contains("green"));
    assertTrue(Arrays.asList(param.value()).contains("genealog"));
  }

  @Test
  public void queryBenchmarkProcessesThousandOperations() throws Exception {
    Method query = QueryBenchmark.class.getMethod("query");
    assertTrue(query.getAnnotation(Benchmark.class) != null);
    assertEquals(QueryFixture.REQUIRED_COUNT,
        query.getAnnotation(OperationsPerInvocation.class).value());
    assertEquals(long.class, query.getReturnType());
    assertTrue(readSource(QueryBenchmark.class).contains(
        "@OperationsPerInvocation(QueryFixture.REQUIRED_COUNT)"));
  }

  private static String readSource(Class<?> type) throws Exception {
    File src = new File("src/main/java/" + type.getName().replace('.', '/') + ".java");
    BufferedReader in = new BufferedReader(
        new InputStreamReader(new FileInputStream(src), StandardCharsets.UTF_8));
    try {
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = in.readLine()) != null) {
        sb.append(line).append('\n');
      }
      return sb.toString();
    } finally {
      in.close();
    }
  }
}
