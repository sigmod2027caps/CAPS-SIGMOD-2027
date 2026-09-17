package io.palyvos.provenance.usecases.nexmark.provenance.queries;

import static io.palyvos.provenance.usecases.nexmark.NexmarkConstants.WINDOW_SIZE;

import io.palyvos.provenance.genealog.GenealogCardinalityDiscardSink;
import io.palyvos.provenance.genealog.GenealogIndexingSink;
import io.palyvos.provenance.genealog.GenealogMetadataProbe;
import io.palyvos.provenance.genealog.GenealogTraversalSink;
import io.palyvos.provenance.genealog.GenealogJoinHelper;
import io.palyvos.provenance.usecases.nexmark.provenance.NexmarkEventTupleGL;
import io.palyvos.provenance.usecases.nexmark.provenance.NexmarkSourceGL;
import io.palyvos.provenance.usecases.nexmark.provenance.NexmarkSuggestionTupleGL;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.util.ProvenanceActivator;
import io.palyvos.provenance.util.TimestampConverter;
import java.util.Arrays;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

/**
 * Nexmark Q3 Local Item Suggestion workflow (nexmark_2) with GeneaLog
 * provenance: who is selling in OR, ID or CA in category 10, and for what
 * auction ids?
 *
 * <pre>
 *   Persons  ── Filter(state ∈ {OR,ID,CA}) ── Map ──┐
 *                                                    Join(person.id = auction.seller, 12h tumbling) ── Sink
 *   Auctions ── Filter(category = 10) ──────── Map ──┘
 * </pre>
 *
 * Output: (personId, name, city, state, auctionId)
 */
public class NexmarkLocalItemSuggestion {

  public static void main(String[] args) throws Exception {
    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final TimestampConverter timestampConverter = ts -> ts;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.setMaxParallelism(settings.maxParallelism());

    FlinkSerializerActivator.PROVENANCE_OPTIMIZED.activate(env, settings);
    final boolean metadataVolume = GenealogMetadataProbe.enabled(settings);

    DataStream<NexmarkEventTupleGL> persons =
        source(env, settings, true, 0, metadataVolume)
            .filter(NexmarkEventTupleGL::inTargetState)
            .name("FILTER-STATE");
    persons = GenealogMetadataProbe.attach(persons, metadataVolume, "FILTER-STATE");
    DataStream<NexmarkEventTupleGL> auctions =
        source(env, settings, false, 1, metadataVolume)
            .filter(e -> e.getCategory() == 10)
            .name("FILTER-CATEGORY");
    auctions = GenealogMetadataProbe.attach(auctions, metadataVolume, "FILTER-CATEGORY");

    DataStream<NexmarkSuggestionTupleGL> joined = persons
        .join(auctions)
        .where(NexmarkEventTupleGL::getPersonId)
        .equalTo(NexmarkEventTupleGL::getPersonId)
        .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
        .apply(new SuggestionJoin());
    joined = GenealogMetadataProbe.attach(joined, metadataVolume, "JOIN");

    if (metadataVolume) {
      joined
          .addSink(new GenealogCardinalityDiscardSink<NexmarkSuggestionTupleGL>())
          .setParallelism(1)
          .name("SINK");
    } else if (settings.indexBenchDir() != null) {
      // Temporal-query bench: store outputs in the GenealogIndex and answer
      // time-range queries with the provenance-graph BFS at end of stream.
      joined
          .addSink(new GenealogIndexingSink<>(settings,
              2 * WINDOW_SIZE.toMilliseconds()))
          .setParallelism(1)
          .name("SINK-INDEX");
    } else if (settings.traversalBenchDir() != null) {
      // Per-tuple provenance bench: keep outputs in memory (no file I/O),
      // sample some at end of stream and time their graph traversal.
      joined
          .addSink(new GenealogTraversalSink<>(settings,
              (t, out) -> {
                out.writeLong(t.getPersonId());
                out.writeLong(t.getTimestamp());
                temporalindex.LogicalSampler.writeString(out, t.getName());
                temporalindex.LogicalSampler.writeString(out, t.getCity());
                temporalindex.LogicalSampler.writeString(out, t.getState());
                out.writeLong(t.getAuctionId());
              }))
          .setParallelism(1)
          .name("SINK-TRAVERSAL");
    } else {
      settings
          .genealogActivator()
          .activate(
              Arrays.asList(ProvenanceActivator.convert(joined)),
              Arrays.asList("JOINED"),
              settings,
              2 * WINDOW_SIZE.toMilliseconds(),
              timestampConverter);
    }

    GenealogMetadataProbe.writeIfEnabled(env.execute("NexmarkLocalItemSuggestion"),
        settings, "nexmark_2");
  }

  private static DataStream<NexmarkEventTupleGL> source(
      StreamExecutionEnvironment env, ExperimentSettings settings,
      boolean personStream, int componentIndex, boolean metadataVolume) {
    DataStream<NexmarkEventTupleGL> sourced = env
        .addSource(new NexmarkSourceGL(settings, personStream))
        .name(personStream ? "SOURCE-PERSONS" : "SOURCE-AUCTIONS")
        .assignTimestampsAndWatermarks(
            new AssignerWithPunctuatedWatermarks<NexmarkEventTupleGL>() {
              @Override
              public long extractTimestamp(NexmarkEventTupleGL t, long prev) {
                return t.getTimestamp();
              }
              @Override
              public Watermark checkAndGetNextWatermark(NexmarkEventTupleGL t, long ts) {
                return new Watermark(ts);
              }
            })
        .setParallelism(1)
        .map(settings.genealogActivator()
            .uidAssigner(componentIndex, settings.maxParallelism()))
        .returns(NexmarkEventTupleGL.class)
        .setParallelism(env.getParallelism());
    return GenealogMetadataProbe.attach(sourced, metadataVolume,
        personStream ? "SOURCE-PERSONS" : "SOURCE-AUCTIONS");
  }

  /**
   * Shapes the output (name, city, state, auction.id) of each joined pair,
   * annotating it with GeneaLog JOIN provenance.
   */
  public static class SuggestionJoin
      implements JoinFunction<NexmarkEventTupleGL, NexmarkEventTupleGL, NexmarkSuggestionTupleGL> {

    @Override
    public NexmarkSuggestionTupleGL join(NexmarkEventTupleGL person,
        NexmarkEventTupleGL auction) {
      NexmarkSuggestionTupleGL result = new NexmarkSuggestionTupleGL(
          person.getPersonId(),
          Math.max(person.getTimestamp(), auction.getTimestamp()),
          Math.max(person.getStimulus(), auction.getStimulus()),
          person.getName(),
          person.getCity(),
          person.getState(),
          auction.getAuctionId());
      GenealogJoinHelper.INSTANCE.annotateResult(person, auction, result);
      return result;
    }
  }
}
