package realscale;

import java.io.Serializable;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import temporalindex.StampDerivedTime;

/**
 * No provenance on real data: the same dataflow without an annotation (the channel is computed by the
 * source like in the other methods, but not stored).
 * Source -> keyBy(key) -> tumbling window -> Agg(count, sum) -> Stamp -> Sink
 *
 * Usage: RealNoprov dataset inputFile outputFile M [file | discard dir]
 * file row: key,timestamp,count,sum
 */
public class RealNoprov {

  public static void main(String[] args) throws Exception {
    final String dataset = args[0];
    final int channels = Integer.parseInt(args[3]);
    final String mode = args.length > 4 ? args[4] : "file";

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.setParallelism(Integer.parseInt(System.getenv().getOrDefault("CAPS_PARALLELISM", "1")));

    DataStream<Record> source = env
        .addSource(new Source(dataset, args[1], channels), TypeInformation.of(Record.class)).name("SOURCE")
        .assignTimestampsAndWatermarks(new RealData.Assigner<Record>());

    DataStream<Out> aggregated = source
        .keyBy(r -> r.key)
        .window(TumblingEventTimeWindows.of(Time.milliseconds(RealData.windowMs(dataset))))
        .aggregate(new CountSumAggregate())
        .name("AGG")
        .process(new StampDerivedTime<Out>((t, ts) -> t.timestamp = ts))
        .returns(Out.class)
        .name("STAMP-AGG");

    if (mode.equals("file")) aggregated.addSink(new RealData.FileSink<Out>(args[2], null, 0)).setParallelism(1).name("SINK");
    else if (mode.equals("discard")) aggregated.addSink(new RealData.DiscardSink<Out>(args[5])).setParallelism(1).name("SINK");
    else throw new IllegalArgumentException("unknown mode " + mode);

    JobExecutionResult result = env.execute("RealScale-noprov");
    System.out.println("Job Runtime: " + result.getNetRuntime() + " ms");
  }

  public static class Record extends RealData.Rec {}

  public static class Out extends RealData.AggOut {
    @Override
    public String row() {
      return head();
    }
  }

  public static class Source extends RealData.LineSource<Record> {
    public Source(String dataset, String path, int channels) {
      super(dataset, path, channels);
    }

    @Override
    protected Record make(long seq, int channel) {
      return new Record();
    }
  }

  public static class CountSumAggregate implements AggregateFunction<Record, CountSumAggregate.Acc, Out> {
    @Override
    public Acc createAccumulator() {
      return new Acc();
    }

    @Override
    public Acc add(Record v, Acc acc) {
      acc.key = v.key;
      acc.count++;
      acc.sum += v.value;
      return acc;
    }

    @Override
    public Out getResult(Acc acc) {
      Out o = new Out();   // timestamp: StampDerivedTime sets the window end after the operator
      o.key = acc.key;
      o.count = acc.count;
      o.sum = acc.sum;
      return o;
    }

    @Override
    public Acc merge(Acc a, Acc b) {
      a.count += b.count;
      a.sum += b.sum;
      return a;
    }

    public static class Acc implements Serializable {
      String key;
      long count;
      long sum;
    }
  }
}
