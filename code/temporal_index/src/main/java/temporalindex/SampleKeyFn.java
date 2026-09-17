package temporalindex;

import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

// Writes the logical-output key bytes for sampling — no provenance fields, no object identity.
// CAPS, Ink, GeneaLog and noprov must hash the same tuple content or the coin diverges.
public interface SampleKeyFn<T> extends Serializable {
  void writeKey(T value, DataOutput out) throws IOException;
}
