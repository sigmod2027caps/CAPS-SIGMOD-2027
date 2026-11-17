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

import ink.InkMeta;
import ink.MonomialBuilder;
import temporalindex.StampDerivedTime;

/**
 * Green (provenance polynomials) on real data: every record is its own variable x_(seq*M + channel), the
 * aggregate multiplies the monomials; the channel counters are the projection of the polynomial at the sink.
 * Source (assign channel) -> keyBy(key) -> tumbling window -> Agg(count, sum, vector +=) -> Stamp -> Sink
 *
 * Usage: RealGreen dataset inputFile outputFile M [file [sampleDir period] | discard dir]
 * file row: key,timestamp,count,sum,c:n;c:n (non-zero channels of the projection),metadataBytes (8 B per variable)
 */
public class RealGreen {

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
        .aggregate(new CountSumAggregate(channels))
        .name("AGG")
        .process(new StampDerivedTime<Out>((t, ts) -> t.timestamp = ts))
        .returns(Out.class)
        .name("STAMP-AGG");

    if (mode.equals("file")) aggregated.addSink(new RealData.FileSink<Out>(args[2], args.length > 5 ? args[5] : null, args.length > 6 ? Long.parseLong(args[6]) : 0)).setParallelism(1).name("SINK");
    else if (mode.equals("discard")) aggregated.addSink(new RealData.DiscardSink<Out>(args[5])).setParallelism(1).name("SINK");
    else throw new IllegalArgumentException("unknown mode " + mode);

    JobExecutionResult result = env.execute("RealScale-Green");
    System.out.println("Job Runtime: " + result.getNetRuntime() + " ms");
  }

  public static class Record extends RealData.Rec {
    public InkMeta hm;
  }

  public static class Out extends RealData.AggOut {
    public InkMeta hm;
    public int channels;

    @Override
    public String row() {
      long[] counts = hm.howMuch(channels);
      StringBuilder sb = new StringBuilder(head()).append(',');
      boolean first = true;
      for (int c = 0; c < counts.length; c++)
        if (counts[c] != 0) {
          if (!first) sb.append(';');
          first = false;
          sb.append(c).append(':').append(counts[c]);
        }
      return sb.append(',').append(hm.metadataBytes()).toString();
    }

    @Override
    public Object sample() {
      return hm;
    }
  }

  public static class Source extends RealData.LineSource<Record> {
    private final int channels;

    public Source(String dataset, String path, int channels) {
      super(dataset, path, channels);
      this.channels = channels;
    }

    @Override
    protected Record make(long seq, int channel) {
      Record r = new Record();
      r.hm = InkMeta.source(channel, channels, seq);
      return r;
    }
  }

  /** Windowed count and sum that also adds up the per-channel counters: multiplies the monomials. */
  public static class CountSumAggregate implements AggregateFunction<Record, CountSumAggregate.Acc, Out> {
    private final int channels;

    public CountSumAggregate(int channels) {
      this.channels = channels;
    }

    @Override
    public Acc createAccumulator() {
      return new Acc();
    }

    @Override
    public Acc add(Record v, Acc acc) {
      acc.key = v.key;
      acc.count++;
      acc.sum += v.value;
      acc.hm.mul(v.hm);
      return acc;
    }

    @Override
    public Out getResult(Acc acc) {
      Out o = new Out();   // timestamp: StampDerivedTime sets the window end after the operator
      o.key = acc.key;
      o.count = acc.count;
      o.sum = acc.sum;
      o.hm = acc.hm.toMeta();
      o.channels = channels;
      return o;
    }

    @Override
    public Acc merge(Acc a, Acc b) {
      a.count += b.count;
      a.sum += b.sum;
      a.hm.mul(b.hm);
      return a;
    }

    public static class Acc implements Serializable {
      String key;
      long count;
      long sum;
      MonomialBuilder hm = new MonomialBuilder();
    }
  }
}
