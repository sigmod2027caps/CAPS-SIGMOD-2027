package temporalindex;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;

// Bounded trace of rows of longs (the E3 traces of watermark steps, arrivals, ...). When it holds maxRows rows it
// drops every other row and from then on keeps one row in `stride`, so a long run still ends with at most
// maxRows evenly spread rows. Rows hold cumulative values, so thinning loses resolution, not totals.
public final class LongTrace {

  private final int cols;
  private final int maxRows;
  private long[] data;
  private int rows;
  private long stride = 1;
  private long seen;

  public LongTrace(int cols, int maxRows) {
    this.cols = cols;
    this.maxRows = maxRows;
    this.data = new long[cols * 256];
  }

  public void add(long... row) {
    if (seen++ % stride != 0)
      return;
    if (rows == maxRows) {
      int kept = 0;
      for (int r = 0; r < rows; r += 2) {
        System.arraycopy(data, r * cols, data, kept * cols, cols);
        kept++;
      }
      rows = kept;
      stride *= 2;
    }
    if ((rows + 1) * cols > data.length)
      data = Arrays.copyOf(data, Math.min(data.length * 2, maxRows * cols));
    System.arraycopy(row, 0, data, rows * cols, cols);
    rows++;
  }

  public int rows() {
    return rows;
  }

  public void write(String path, String header) throws IOException {
    try (PrintWriter pw = new PrintWriter(new FileWriter(path))) {
      pw.println(header);
      StringBuilder sb = new StringBuilder();
      for (int r = 0; r < rows; r++) {
        sb.setLength(0);
        for (int c = 0; c < cols; c++)
          sb.append(c == 0 ? "" : ",").append(data[r * cols + c]);
        pw.println(sb);
      }
    }
  }
}
