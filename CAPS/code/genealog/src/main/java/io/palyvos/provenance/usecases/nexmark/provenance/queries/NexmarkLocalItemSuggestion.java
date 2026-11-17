package io.palyvos.provenance.usecases.nexmark.provenance.queries;

import static io.palyvos.provenance.usecases.nexmark.NexmarkConstants.WINDOW_SIZE;

import io.palyvos.provenance.genealog.GenealogCardinalityDiscardSink;
import io.palyvos.provenance.genealog.GenealogIndexingSink;
import io.palyvos.provenance.genealog.GenealogMetadataProbe;
import io.palyvos.provenance.genealog.GenealogTraversalSink;
import io.palyvos.provenance.genealog.GenealogJoinHelper;
import io.palyvos.provenance.genealog.GenealogLiveSink;
import io.palyvos.provenance.usecases.nexmark.provenance.NexmarkEventTupleGL;
import io.palyvos.provenance.usecases.nexmark.provenance.NexmarkSourceGL;
import io.palyvos.provenance.usecases.nexmark.provenance.NexmarkSuggestionTupleGL;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.util.ProvenanceActivator;
import io.palyvos.provenance.util.TimestampConverter;
import temporalindex.Disorder;
import temporalindex.DisorderConfig;
import temporalindex.LiveState;
import temporalindex.StampDerivedTime;
import java.util.Arrays;
import org.apache.flink.api.common.functions.JoinFunction;
import java.io.File;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

/**
 * Nexmark Q3 Local Item Suggestion workflow (nexmark_2) with GeneaLog
 * provenance: who is selling in OR, ID or CA in category 10, and for what
 * auction ids?
 *
 * <pre>
 *   Persons  -- Filter(state in {OR,ID,CA}) -- Map --+
 *                                                    Join(person.id = auction.seller, 12h tumbling) -- Sink
 *   Auctions -- Filter(category = 10) -------- Map --+
 * </pre>
 *
 * Output: (personId, name, city, state, auctionId)
 */
public class NexmarkLocalItemSuggestion {

  public static void main(String[] args) throws Exception {
    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    // The same environment, reader and watermark assigner as the CAPS, Green and no-provenance jobs (CAPS_* variables).
    final DisorderConfig cfg = DisorderConfig.fromEnv();
    final StreamExecutionEnvironment env = Disorder.environment(cfg);
    final TimestampConverter timestampConverter = ts -> ts;
    env.setMaxParallelism(settings.maxParallelism());

    FlinkSerializerActivator.PROVENANCE_OPTIMIZED.activate(env, settings);
    // live_memory: CAPS_LIVE_STATE=1 and --metadataVolumeDir <dir>; the dedicated sink publishes the live state.
    final boolean live = LiveState.on() && settings.metadataVolumeDir() != null;
    final boolean metadataVolume = GenealogMetadataProbe.enabled(settings) && !live;

    DataStream<NexmarkEventTupleGL> persons =
        source(env, cfg, settings, true, 0, metadataVolume)
            .filter(NexmarkEventTupleGL::inTargetState)
            .name("FILTER-STATE");
    persons = GenealogMetadataProbe.attach(persons, metadataVolume, "FILTER-STATE");
    DataStream<NexmarkEventTupleGL> auctions =
        source(env, cfg, settings, false, 1, metadataVolume)
            .filter(e -> e.getCategory() == 10)
            .name("FILTER-CATEGORY");
    auctions = GenealogMetadataProbe.attach(auctions, metadataVolume, "FILTER-CATEGORY");

    DataStream<NexmarkSuggestionTupleGL> joined = persons
        .join(auctions)
        .where(NexmarkEventTupleGL::getPersonId)
        .equalTo(NexmarkEventTupleGL::getPersonId)
        .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "JOIN", 0, WINDOW_SIZE, null, GenealogLiveSink.TUPLE_ONLY, null, GenealogLiveSink.REMOTE_SET))
        .apply(new SuggestionJoin())
        .process(new StampDerivedTime<NexmarkSuggestionTupleGL>((t, ts) -> t.setTimestamp(ts)))
        .returns(NexmarkSuggestionTupleGL.class)
        .name("STAMP-JOIN");
    joined = GenealogMetadataProbe.attach(joined, metadataVolume, "JOIN");

    if (live) {
      joined
          .addSink(new GenealogLiveSink<NexmarkSuggestionTupleGL>(settings))
          .name("SINK");
    } else if (metadataVolume) {
      joined
          .addSink(new GenealogCardinalityDiscardSink<NexmarkSuggestionTupleGL>())
          .setParallelism(1)
          .name("SINK");
    } else if (settings.indexBenchDir() != null) {
      // Temporal-query bench: store outputs in the GenealogIndex and answer
      // time-range queries with the provenance-graph BFS at end of stream.
      // Watermark-driven Tier 1 -> ordered append (as in CAPS), one shard per sink subtask.
      GenealogIndexingSink.attach(joined, new GenealogIndexingSink<>(settings), "SINK-INDEX");
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

    JobExecutionResult result = env.execute("NexmarkLocalItemSuggestion");
    if (live)
      LiveState.write(result.getAllAccumulatorResults(), new File(settings.metadataVolumeDir()), "nexmark_2", "genealog");
    else
      GenealogMetadataProbe.writeIfEnabled(result, settings, "nexmark_2");
  }

  private static DataStream<NexmarkEventTupleGL> source(
      StreamExecutionEnvironment env, DisorderConfig cfg, ExperimentSettings settings,
      boolean personStream, int componentIndex, boolean metadataVolume) {
    DataStream<NexmarkEventTupleGL> sourced = NexmarkSourceGL.read(env, cfg, settings, personStream)
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
          // Placeholder: StampDerivedTime sets the window end after the join.
          0,
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
