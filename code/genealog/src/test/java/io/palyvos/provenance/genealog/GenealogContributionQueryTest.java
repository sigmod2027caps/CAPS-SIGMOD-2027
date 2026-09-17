package io.palyvos.provenance.genealog;

import static org.junit.Assert.assertEquals;

import io.palyvos.provenance.ananke.aggregate.SortedPointersAggregateStrategy;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import java.util.ArrayList;
import org.junit.Test;

public class GenealogContributionQueryTest {

  @Test
  public void countsSourceAndMetaSourceLeaves() {
    assertEquals(1L, GenealogContributionQuery.count(leaf(GenealogTupleType.SOURCE), strategy()));
    assertEquals(1L, GenealogContributionQuery.count(leaf(GenealogTupleType.META_SOURCE), strategy()));
  }

  @Test
  public void followsMapJoinAggregateAndRemote() {
    Stub srcA = leaf(GenealogTupleType.SOURCE);
    Stub srcB = leaf(GenealogTupleType.SOURCE);
    Stub mapped = typed(GenealogTupleType.MAP);
    mapped.setU1(srcA);

    Stub joined = typed(GenealogTupleType.JOIN);
    joined.setU1(mapped);
    joined.setU2(srcB);
    assertEquals(2L, GenealogContributionQuery.count(joined, strategy()));

    Stub s1 = leaf(GenealogTupleType.SOURCE);
    Stub s2 = leaf(GenealogTupleType.SOURCE);
    Stub s3 = leaf(GenealogTupleType.SOURCE);
    s1.setNext(s2);
    s2.setNext(s3);
    Stub agg = typed(GenealogTupleType.AGGREGATE);
    agg.setU2(s1);
    agg.setU1(s3);
    assertEquals(3L, GenealogContributionQuery.count(agg, strategy()));

    Stub remote = typed(GenealogTupleType.REMOTE);
    ArrayList<TimestampedUIDTuple> proven = new ArrayList<TimestampedUIDTuple>();
    proven.add(leaf(GenealogTupleType.SOURCE));
    proven.add(leaf(GenealogTupleType.SOURCE));
    proven.add(leaf(GenealogTupleType.SOURCE));
    proven.add(leaf(GenealogTupleType.SOURCE));
    remote.getGenealogData().setProvenance(proven);
    assertEquals(4L, GenealogContributionQuery.count(remote, strategy()));
  }

  static SortedPointersAggregateStrategy strategy() {
    return new SortedPointersAggregateStrategy();
  }

  static Stub leaf(GenealogTupleType type) {
    return typed(type);
  }

  static Stub typed(GenealogTupleType type) {
    Stub t = new Stub();
    t.initGenealog(type);
    return t;
  }

  static final class Stub implements GenealogTuple {
    private GenealogData gdata;
    private long timestamp;

    @Override
    public void initGenealog(GenealogTupleType tupleType) {
      gdata = new GenealogData();
      gdata.init(tupleType);
    }

    @Override
    public GenealogData getGenealogData() {
      return gdata;
    }

    @Override
    public long getTimestamp() {
      return timestamp;
    }

    @Override
    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }

    @Override
    public long getStimulus() {
      return 0;
    }

    @Override
    public void setStimulus(long stimulus) {
    }
  }
}
