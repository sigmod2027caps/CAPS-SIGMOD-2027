#!/usr/bin/env python3
"""Validate shared-query-ID manifests, answers, and JMH query CSVs."""
from __future__ import annotations

import csv
import math
import sys
from pathlib import Path

REQUIRED = 1000
ANSWER_HEADER = "query_id,total_contributions"
JMH_HEADER = "method,score_ns,error_ns,samples,unit,checksum,fixture_count,id_checksum"
JMH_SAMPLES = 5
JMH_UNIT = "ns/op"
METHODS = ("caps", "green", "genealog")
ANSWER_FILES = {
    "caps": "query_answers_caps.csv",
    "green": "query_answers_green.csv",
    "genealog": "query_answers_genealog.csv",
}
JMH_FILE = "jmh_query.csv"
HEADER_PREFIX = "# count="
HEADER_SEED = " seed="
MANIFEST_SEED = 42
INT64_MIN = -(1 << 63)
INT64_MAX = (1 << 63) - 1


class ValidationError(Exception):
    pass


def _fail(message):
    raise ValidationError(message)


def _read_lines(path, label):
    if not path.is_file():
        _fail("missing %s: %s" % (label, path))
    text = path.read_text()
    if text == "":
        _fail("empty %s: %s" % (label, path))
    return text.splitlines()


def _parse_int64(text, label):
    try:
        value = int(text, 10)
    except ValueError:
        _fail("malformed %s: %r" % (label, text))
    if text != str(value) or value < INT64_MIN or value > INT64_MAX:
        _fail("malformed %s: %r" % (label, text))
    return value


def load_manifest(path):
    path = Path(path)
    lines = _read_lines(path, "manifest")
    header = lines[0]
    if not header.startswith(HEADER_PREFIX) or HEADER_SEED not in header:
        _fail("malformed manifest header: %s" % path)
    seed_at = header.find(HEADER_SEED)
    if header.find(HEADER_SEED, seed_at + len(HEADER_SEED)) >= 0:
        _fail("malformed manifest header: %s" % path)
    count_text = header[len(HEADER_PREFIX):seed_at]
    seed_text = header[seed_at + len(HEADER_SEED):]
    count = _parse_int64(count_text, "manifest header count")
    seed = _parse_int64(seed_text, "manifest header seed")
    if count != REQUIRED:
        _fail("manifest header count=%d, want %d: %s" % (count, REQUIRED, path))
    if seed != MANIFEST_SEED:
        _fail("manifest header seed=%d, want %d: %s" % (seed, MANIFEST_SEED, path))
    ids = []
    seen = set()
    for line in lines[1:]:
        if line == "" or " " in line or "\t" in line:
            _fail("malformed manifest id line: %r" % line)
        query_id = _parse_int64(line, "manifest id")
        if query_id in seen:
            _fail("duplicate manifest id %d" % query_id)
        seen.add(query_id)
        ids.append(query_id)
    if len(ids) != REQUIRED:
        _fail("manifest has %d ids, want count=%d" % (len(ids), REQUIRED))
    return ids


def load_answers(path):
    path = Path(path)
    lines = _read_lines(path, "answer file")
    if lines[0] != ANSWER_HEADER:
        _fail("answer file header must be %s: %s" % (ANSWER_HEADER, path))
    answers = {}
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) != 2:
            _fail("malformed answer row %r in %s" % (line, path))
        query_id = _parse_int64(parts[0], "answer query_id")
        value = _parse_int64(parts[1], "answer total_contributions")
        if query_id in answers:
            _fail("duplicate answer id %d in %s" % (query_id, path))
        answers[query_id] = value
    if len(answers) != REQUIRED:
        _fail("answer file has %d unique ids, want %d: %s"
              % (len(answers), REQUIRED, path))
    return answers


def java_long_sum(values):
    total = 0
    for value in values:
        total = ((total + int(value) + (1 << 63)) % (1 << 64)) - (1 << 63)
    return total


def _parse_finite_nonneg(text, label, path):
    try:
        value = float(text)
    except (TypeError, ValueError):
        _fail("malformed %s %r in %s" % (label, text, path))
    if not math.isfinite(value) or value < 0:
        _fail("%s must be finite nonnegative, got %r in %s" % (label, text, path))
    return text


def load_jmh(path, method):
    path = Path(path)
    lines = _read_lines(path, "JMH CSV")
    if lines[0] != JMH_HEADER:
        _fail("malformed JMH CSV header: %s" % path)
    if len(lines) < 2:
        _fail("empty JMH CSV rows: %s" % path)
    row = next(csv.DictReader(lines))
    if row.get("method") != method:
        _fail("JMH method %r, want %r: %s" % (row.get("method"), method, path))
    if row.get("samples") != str(JMH_SAMPLES):
        _fail("JMH samples=%s, want %d: %s"
              % (row.get("samples"), JMH_SAMPLES, path))
    if row.get("unit") != JMH_UNIT:
        _fail("JMH unit=%r, want %s: %s" % (row.get("unit"), JMH_UNIT, path))
    score = _parse_finite_nonneg(row.get("score_ns"), "score", path)
    error = _parse_finite_nonneg(row.get("error_ns"), "error", path)
    checksum = _parse_int64(row.get("checksum"), "checksum")
    fixture_count = _parse_int64(row.get("fixture_count"), "fixture_count")
    if fixture_count != REQUIRED:
        _fail("JMH fixture_count=%s, want %d: %s"
              % (row.get("fixture_count"), REQUIRED, path))
    id_checksum = _parse_int64(row.get("id_checksum"), "id_checksum")
    return {
        "method": method,
        "score_ns": score,
        "error_ns": error,
        "checksum": str(checksum),
        "fixture_count": str(fixture_count),
        "id_checksum": str(id_checksum),
    }


def validate_dataflow(expdir):
    expdir = Path(expdir)
    manifest_ids = load_manifest(expdir / "query_ids.txt")
    manifest_set = set(manifest_ids)
    answers = {}
    for method in METHODS:
        answers[method] = load_answers(expdir / method / ANSWER_FILES[method])
        if set(answers[method]) != manifest_set:
            _fail("%s answer ids mismatch manifest / are not identical" % method)
    reference = answers["caps"]
    for method in METHODS[1:]:
        for query_id, value in reference.items():
            other = answers[method][query_id]
            if other != value:
                _fail("answer mismatch for id %d: caps=%s %s=%s"
                      % (query_id, value, method, other))
    want_id_checksum = java_long_sum(manifest_ids)
    for method in METHODS:
        jmh = load_jmh(expdir / method / JMH_FILE, method)
        want_checksum = java_long_sum(answers[method].values())
        if int(jmh["checksum"]) != want_checksum:
            _fail("%s JMH checksum %s != answer sum %s"
                  % (method, jmh["checksum"], want_checksum))
        if int(jmh["id_checksum"]) != want_id_checksum:
            _fail("%s JMH id_checksum %s != manifest id checksum %s"
                  % (method, jmh["id_checksum"], want_id_checksum))


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    try:
        if len(argv) != 1:
            _fail("usage: validate.py <dataflow_dir>")
        validate_dataflow(argv[0])
    except ValidationError as exc:
        print("FAIL: %s" % exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
