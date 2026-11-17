package tieredindex;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JVM-wide meeting point of the subtasks of one sink. The experiment harness runs every job on a
 * single TaskManager, so all K subtasks of a sink live in the same JVM: each subtask finishes its
 * own shard (flush with the final watermark) and calls arrive(); the last one to arrive gets all K
 * shards and assembles them (ShardedIndex, merged statistics, benchmark queries). A deployment on
 * several TaskManagers has no shared JVM: there the shards would be queried through a query
 * service that merges the shard answers exactly like ShardedIndex does (channel-wise sum, frontier =
 * min shard watermark). Per-output provenance queries need no merge at all: the vector rides on the tuple.
 * At parallelism above 1: how the sink subtasks of one job hand over their shards.
 */
public final class ShardGroup {

  private static final Map<String, Object[]> GROUPS = new HashMap<>();

  private ShardGroup() {}

  /**
   * Registers subtask `subtask` of `parallelism` under `key` (unique per sink instance of one job).
   * Returns null unless this is the last of the subtasks to arrive; then the shards, ordered by
   * subtask index, and the group is forgotten.
   */
  @SuppressWarnings("unchecked")
  public static synchronized <S> List<S> arrive(String key, int parallelism, int subtask, S shard) {
    Object[] g = GROUPS.computeIfAbsent(key, k -> new Object[parallelism]);
    if (g.length != parallelism || subtask < 0 || subtask >= parallelism || g[subtask] != null)
      throw new IllegalStateException("shard group " + key + ": bad or repeated subtask " + subtask);
    g[subtask] = shard;
    for (Object o : g)
      if (o == null)
        return null;
    GROUPS.remove(key);
    return (List<S>) Arrays.asList(g);
  }
}
