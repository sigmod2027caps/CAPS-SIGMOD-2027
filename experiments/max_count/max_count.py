"""Largest per-channel count of CAPS over all sink outputs. See run.sh."""
import argparse
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common


def scan(path, channels):
    """(outputs, largest counter, its channel). The last `channels` columns of a row are the counters; O(outputs) time, O(1) space."""
    outputs, largest, where = 0, -1, -1
    with open(path) as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("---"):
                continue
            outputs += 1
            cols = line.rsplit(",", channels)
            if len(cols) != channels + 1:
                raise ValueError("%s: fewer than %d counters: %s" % (path, channels, line))
            for k, count in enumerate(cols[1:]):
                if int(count) > largest:
                    largest, where = int(count), k
    return outputs, largest, where


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--pdf", required=True)
    parser.add_argument("--verify", required=True, help="verify_<tag>.csv of experiments/verify, for the number of outputs")
    parser.add_argument("runs", nargs="+", help="dataflow:channels:path")
    args = parser.parse_args()

    expected = {r["dataflow"]: int(r["outputs"]) for r in common.read_csv(args.verify) if r["check"] == "outputs"}
    rows, failed = [], False
    for run in sorted(args.runs):
        dataflow, channels, path = run.split(":", 2)
        outputs, largest, where = scan(path, int(channels))
        status = "PASS" if outputs == expected.get(dataflow) else "FAIL"
        failed |= status == "FAIL"
        rows.append([dataflow, channels, outputs, largest, where, math.ceil(math.log2(largest + 1)), status])
    common.write_csv(args.csv, ["dataflow", "channels", "outputs", "max_count", "max_channel", "bits", "outputs_match_verify"], rows)

    common.setup_style()
    fig, axes = common.new_figure(1)
    ax = axes[0]
    ax.bar([common.DATAFLOW_NAMES.get(r[0], r[0]) for r in rows], [max(r[3], 1) for r in rows], width=common.BAR_WIDTH, color=common.METHOD_COLORS["caps"],
           hatch=common.METHOD_HATCHES["caps"], edgecolor="black", linewidth=0.5)
    ax.set_ylabel("Largest count")
    ax.tick_params(axis="x", rotation=60)
    common.style_axis(ax, log_y=True)
    common.save_pdf(fig, args.pdf)
    sys.exit(1 if failed else 0)
