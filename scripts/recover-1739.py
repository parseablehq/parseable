"""
Recovery tool for parseablehq/parseable#1739 - duplicated manifest_list entries.

A writer whose hostname changed stops recognising its own manifest entry. It appends a new
snapshot entry every sync, and rewrites its manifest file with only that sync's parquet -
so statistics inflate and every earlier parquet file for that run becomes unreferenced.

Two repairs, both needed:

  Tier 1  statistics   collapse duplicate snapshot entries          (default)
  Tier 2  manifests    fold orphaned parquet back into the writer's  (--rebuild)
                       own manifest, and fix broken file paths, so
                       the data becomes queryable again

Tier 2 rewrites each affected manifest IN PLACE (no new object) after backing up the
original, and re-running is safe: it repairs any manifest a previous, buggy run left with
broken paths, and is a no-op once everything resolves.

Tier 1 formula
--------------
Each duplicate entry is a sample of a per-date counter which, at each process start, was
rehydrated by load_daily_metrics() to the sum of every entry then present. So for the
contiguous runs of entries sharing a manifest_path within one date:

    real(k) = max(run k) - sum(every entry in runs 1..k-1)
    total   = sum of real(k)

Verified exact against a confirmed reproduction on all three metrics.

Limitation: this holds only where load_daily_metrics() actually rehydrated the counter, so
every run's peak sits above the total preceding it. A run peaking below that total does not
fit the model - the tool clamps it to zero, flags it with `!`, and that date's figure is
then a LOWER BOUND. Use --rebuild to get true event and storage numbers for those dates.

Tier 2 caveats
--------------
  * Rebuilt manifests carry NO column min/max statistics. Deriving them in Python risks
    emitting a wrong range, which would silently prune real rows out of query results.
    Null stats are always correct, just without min/max pushdown - recovered dates scan
    slightly more. Fix properly in a Rust admin command if the cost matters.
  * sort_order_id is left empty; the pyarrow API does not expose row-group sorting columns.
    Same trade: correctness kept, a planner optimisation lost.
  * Skips today's partition unless --force-today, because a live node holds the snapshot
    in memory and will write its own copy back over the repair.
  * Refuses streams with more than one stream.json (distributed): attributing orphans across
    several writers needs a decision about which node owns the repair.
  * Orphans whose writer manifest no longer exists cannot be re-attached (this tool never
    creates a manifest object); they are reported, not silently dropped, and need a Rust
    admin command that can also compute real column statistics.

Usage
-----
    # inspect everything, change nothing
    ./recover-1739.py --backend gcs --bucket my-bucket

    # audit which parquet is unreferenced
    ./recover-1739.py --backend s3 --bucket my-bucket --check-orphans

    # repair statistics only
    ./recover-1739.py --backend gcs --bucket my-bucket --apply

    # full repair: statistics + manifests + orphaned parquet
    ./recover-1739.py --backend gcs --bucket my-bucket --rebuild --apply

    # local filesystem store, or a synced copy of the bucket
    ./recover-1739.py --backend local --root /var/lib/parseable

--rebuild needs `pip install pyarrow zstandard`.

STOP INGESTION to the affected streams before --apply.

Credentials
-----------
Never passed as arguments - anything in argv is visible to `ps` and lands in shell history.
Resolution order, first match wins:

  1. Whatever `aws` / `gsutil` already use: profiles, instance/workload identity, gcloud
     ADC. Nothing to configure, and the script never sees a secret. Prefer this.
  2. Standard SDK environment: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN,
     AWS_DEFAULT_REGION / GOOGLE_APPLICATION_CREDENTIALS.
  3. Parseable's own server variables, so you can reuse the deployment's config verbatim:
     P_S3_ACCESS_KEY, P_S3_SECRET_KEY, P_S3_REGION, P_S3_URL. These are mapped into the
     child process environment only.

`--env-file` reads a Parseable-style KEY=VALUE file (the same one the server is given) and
applies it before step 3, so a support engineer can run:

    ./recover-1739.py --env-file .parseable.env --backend s3 --bucket parseable

Non-AWS S3 (MinIO, Ceph, R2, Wasabi) needs an endpoint: taken from P_S3_URL, or set it with
--endpoint-url. Path-style addressing may also need:

    aws configure set default.s3.addressing_style path

GCS has no key variables - Parseable uses application default credentials there, exactly as
gsutil does, so authenticate with `gcloud auth application-default login` or point
GOOGLE_APPLICATION_CREDENTIALS at a service account key.
"""

import argparse
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from collections import defaultdict

METRICS = ("events_ingested", "ingestion_size", "storage_size")
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
ZSTD_LEVEL = 3
MANIFEST_VERSION = "v1"


# --------------------------------------------------------------------------- storage


class Storage:
    """Minimal object-store access. Shells out to the vendor CLI so there are no deps."""

    def __init__(self, backend, bucket=None, root=None, prefix="", endpoint=None, env=None):
        self.backend = backend
        self.bucket = bucket
        self.root = root
        self.prefix = prefix.strip("/")
        self.endpoint = endpoint
        # Credentials live here, in the child process environment only - never in argv,
        # where `ps` and shell history would expose them.
        self.env = env or os.environ.copy()
        if backend == "local":
            if not root:
                sys.exit("--backend local needs --root")
        elif not bucket:
            sys.exit(f"--backend {backend} needs --bucket")
        else:
            tool = {"gcs": "gsutil", "s3": "aws"}[backend]
            if not shutil.which(tool):
                sys.exit(f"{tool} not found on PATH")

    def _aws(self, *args):
        cmd = ["aws", "s3", *args]
        if self.endpoint:
            cmd += ["--endpoint-url", self.endpoint]
        return cmd

    def _url(self, key):
        key = "/".join(p for p in (self.prefix, key) if p)
        if self.backend == "gcs":
            return f"gs://{self.bucket}/{key}"
        if self.backend == "s3":
            return f"s3://{self.bucket}/{key}"
        return os.path.join(self.root, key)

    def _run(self, cmd, **kw):
        return subprocess.run(cmd, capture_output=True, check=False, env=self.env, **kw)

    def ls(self, key_prefix):
        """List keys under a prefix. Returns store-relative keys."""
        url = self._url(key_prefix)
        if self.backend == "local":
            out = []
            base = os.path.join(self.root, self.prefix)
            for dirpath, _, files in os.walk(url):
                for f in files:
                    out.append(os.path.relpath(os.path.join(dirpath, f), base))
            return out
        cmd = (
            ["gsutil", "ls", "-r", url + "**"]
            if self.backend == "gcs"
            else self._aws("ls", "--recursive", url)
        )
        res = self._run(cmd)
        if res.returncode != 0:
            return []
        keys = []
        base = f"{'gs' if self.backend == 'gcs' else 's3'}://{self.bucket}/"
        for line in res.stdout.decode(errors="replace").splitlines():
            line = line.strip()
            if not line:
                continue
            if self.backend == "gcs":
                if not line.startswith(base) or line.endswith("/"):
                    continue
                k = line[len(base) :]
            else:
                parts = line.split(None, 3)
                if len(parts) < 4:
                    continue
                k = parts[3]
            if self.prefix and k.startswith(self.prefix + "/"):
                k = k[len(self.prefix) + 1 :]
            keys.append(k)
        return keys

    def read(self, key):
        url = self._url(key)
        if self.backend == "local":
            with open(url, "rb") as fh:
                return fh.read()
        cmd = ["gsutil", "cat", url] if self.backend == "gcs" else self._aws("cp", url, "-")
        res = self._run(cmd)
        if res.returncode != 0:
            raise IOError(f"read {url}: {res.stderr.decode(errors='replace').strip()}")
        return res.stdout

    def write(self, key, data):
        url = self._url(key)
        if self.backend == "local":
            os.makedirs(os.path.dirname(url), exist_ok=True)
            with open(url, "wb") as fh:
                fh.write(data)
            return
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(data)
            tmp_path = tmp.name
        try:
            cmd = (
                ["gsutil", "cp", tmp_path, url]
                if self.backend == "gcs"
                else self._aws("cp", tmp_path, url)
            )
            res = self._run(cmd)
            if res.returncode != 0:
                raise IOError(f"write {url}: {res.stderr.decode(errors='replace').strip()}")
        finally:
            os.unlink(tmp_path)


# ------------------------------------------------------------------- manifest codec


def decode_manifest(raw):
    """Manifests are zstd since v2.x; older ones are plain JSON. Same filename either way."""
    if raw[:4] != ZSTD_MAGIC:
        return json.loads(raw)
    try:
        import zstandard
    except ImportError:
        if shutil.which("zstd"):
            res = subprocess.run(["zstd", "-dc"], input=raw, capture_output=True)
            if res.returncode == 0:
                return json.loads(res.stdout)
        raise RuntimeError("need `pip install zstandard` or the `zstd` binary")
    return json.loads(zstandard.ZstdDecompressor().decompress(raw, max_output_size=1 << 30))


def encode_manifest(manifest):
    import zstandard

    return zstandard.ZstdCompressor(level=ZSTD_LEVEL).compress(
        json.dumps(manifest, separators=(",", ":")).encode()
    )


# ------------------------------------------------------------- tier 1: collapse stats


def split_runs(entries):
    """Contiguous runs sharing a manifest_path. A value dropping below its predecessor means
    the counter reset - a new process lifetime reusing an earlier hostname - so that starts
    a new run too."""
    runs = []
    for e in entries:
        if runs:
            path, run = runs[-1]
            if path == e["manifest_path"] and all(
                e.get(m, 0) >= run[-1].get(m, 0) for m in METRICS
            ):
                run.append(e)
                continue
        runs.append((e["manifest_path"], [e]))
    return runs


def collapse_date(entries):
    """Collapse one date's entries. Returns (new_entries, notes)."""
    runs = split_runs(entries)
    if len(runs) == len(entries):
        return entries, []

    notes, out = [], []
    prior = {m: 0 for m in METRICS}
    for path, run in runs:
        new = dict(run[-1])
        new["manifest_path"] = path
        for m in METRICS:
            peak = max(e.get(m, 0) for e in run)
            real = peak - prior[m]
            if real < 0:
                # The run peaked below the total of everything before it, so this writer's
                # counter was never rehydrated to that total - the snapshot does not fit the
                # model and the real contribution cannot be derived from it. Clamping makes
                # the result a FLOOR, not the true value.
                notes.append(
                    f"    ! {path.rsplit('/', 1)[-1]}: {m} run peak {peak:,} is below the "
                    f"{prior[m]:,} preceding it; clamped to 0, result is a lower bound"
                )
                real = 0
            new[m] = real
            prior[m] += sum(e.get(m, 0) for e in run)
        out.append(new)
    return out, notes


def group_by_date(manifest_list):
    """(lower, upper) -> entries, preserving first-seen order."""
    buckets, order = defaultdict(list), []
    for e in manifest_list:
        key = (e.get("time_lower_bound"), e.get("time_upper_bound"))
        if key not in buckets:
            order.append(key)
        buckets[key].append(e)
    return order, buckets


def collapse_snapshot(manifest_list):
    order, buckets = group_by_date(manifest_list)
    new_list, notes = [], []
    for key in order:
        collapsed, n = collapse_date(buckets[key])
        new_list.extend(collapsed)
        notes.extend(n)
    return new_list, notes


def totals(entries):
    return {m: sum(e.get(m, 0) for e in entries) for m in METRICS}


# ------------------------------------------------- tier 2: rebuild manifests + orphans


def date_of(entry):
    lb = entry.get("time_lower_bound")
    return lb[:10] if isinstance(lb, str) else None


def scan_date(store, stream, date):
    """List a date partition. Returns (parquet_keys, manifest_keys) as store-relative keys.

    Backups written by this tool end in `.bak-<ts>` and are excluded so a re-run never treats
    them as live manifests or parquet.
    """
    keys = store.ls(f"{stream}/date={date}/")
    parquet = sorted(k for k in keys if k.endswith(".parquet"))
    manifests = sorted(k for k in keys if k.endswith("manifest.json"))
    return parquet, manifests


def manifest_referenced_bases(store, manifest_keys):
    """Basenames of every parquet listed across a date's manifests. Used only for the audit
    headline; path correctness is handled separately in plan_rebuild_date."""
    referenced = set()
    for mk in manifest_keys:
        try:
            man = decode_manifest(store.read(mk))
        except Exception as exc:
            print(f"    cannot read {mk.rsplit('/', 1)[-1]}: {exc}")
            continue
        for f in man.get("files", []):
            referenced.add(f.get("file_path", "").rsplit("/", 1)[-1])
    return referenced


def infer_path_prefix(entries, manifest_keys):
    """Prefix that turns a store key into the path form used inside the snapshot.

    absolute_url() is `Path::parse(prefix)` for s3/gcs, i.e. store-relative with no scheme
    or bucket, so the prefix is usually "". localfs can produce an absolute path instead.
    Derive it by matching a known key as a SUFFIX of a known manifest_path - exact, with no
    parsing of a format that varies by backend.

    Returns None when nothing matches; "" is a valid answer, so test with `is None`.
    """
    paths = [e.get("manifest_path", "") for e in entries]
    for key in manifest_keys:
        for p in paths:
            if p == key:
                return ""
            if p.endswith("/" + key):
                return p[: len(p) - len(key)]
    return None


def build_file_entry(store, key, path_prefix):
    """Read a parquet footer and produce a manifest File entry."""
    import pyarrow.parquet as pq

    raw = store.read(key)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp.write(raw)
        tmp_path = tmp.name
    try:
        md = pq.ParquetFile(tmp_path).metadata
        cols, ingestion = {}, 0
        for rg_idx in range(md.num_row_groups):
            rg = md.row_group(rg_idx)
            ingestion += rg.total_byte_size
            for c_idx in range(rg.num_columns):
                col = rg.column(c_idx)
                name = col.path_in_schema
                if name in cols:
                    cols[name]["compressed_size"] += col.total_compressed_size
                    cols[name]["uncompressed_size"] += col.total_uncompressed_size
                else:
                    cols[name] = {
                        "name": name,
                        # Deliberately null - see module docstring.
                        "stats": None,
                        "uncompressed_size": col.total_uncompressed_size,
                        "compressed_size": col.total_compressed_size,
                    }
        return {
            "file_path": path_prefix + key,
            "num_rows": md.num_rows,
            "file_size": len(raw),
            "ingestion_size": ingestion,
            "columns": list(cols.values()),
            "sort_order_id": [],
        }
    finally:
        os.unlink(tmp_path)


def parquet_writer_token(name):
    """Writer identity embedded in a parquet filename.

    filename_by_partition() builds `<hash>.date=..hour=..minute=..[k=v.]*<hostname><id>.data.`
    and arrow_path_to_parquet() keeps everything up to `.data`, so the dot-delimited token
    immediately before `.data.` is the writer. Hostnames cannot contain a dot - the
    `matches()` filter in the server strips them - so this is unambiguous.
    """
    marker = ".data."
    if marker not in name:
        return None
    return name[: name.index(marker)].rsplit(".", 1)[-1] or None


def manifest_writer_token(name):
    """Same identity, as spelled in a manifest object name.

    `{hostname}.manifest.json` or `ingestor.{hostname}.{node_id}.manifest.json`; the parquet
    side concatenates hostname and node_id with no separator, so drop the dots to compare.
    """
    stem = name.rsplit("/", 1)[-1]
    if not stem.endswith(".manifest.json"):
        return None
    stem = stem[: -len(".manifest.json")].removeprefix("ingestor.")
    return stem.replace(".", "") or None


def plan_rebuild_date(store, stream, date, parquet_keys, manifest_keys, path_prefix):
    """Plan the manifest repair for one date - reads only, writes nothing.

    Two independent defects are repaired:

      * orphaned parquet - a file present on the store that no manifest lists (by basename).
        Routed back to the manifest of the writer that produced it, via the token embedded in
        the filename, and its footer is read to build a File entry.
      * mispathed entries - a manifest File whose `file_path` does not resolve to the object
        that is actually there. This is what a re-run over manifests corrupted by an earlier
        buggy version has to fix; orphan-by-basename detection alone cannot see it, because
        the broken entry still carries the right basename. The path is rewritten to the
        canonical `path_prefix + <store key>`.

    Existing manifests are rewritten in place - no new object - so nothing is left for the
    prefix-listing path in get_manifest_files_for_dates() to double count.

    Returns (plans, unassigned):
      plans     - {manifest_key: {raw, files, added, corrected, events, storage, count}}
      unassigned- orphan parquet keys whose writer's manifest no longer exists
    """
    key_by_base = {k.rsplit("/", 1)[-1]: k for k in parquet_keys}

    # Load every manifest once; keep raw bytes for the backup.
    loaded = {}
    referenced_bases = set()
    for mk in sorted(set(manifest_keys)):
        try:
            raw = store.read(mk)
            files = decode_manifest(raw).get("files", [])
        except Exception as exc:
            print(f"    cannot read {mk.rsplit('/', 1)[-1]}: {exc}")
            continue
        loaded[mk] = (raw, files)
        for f in files:
            referenced_bases.add(f.get("file_path", "").rsplit("/", 1)[-1])

    owners = {}
    for mk in loaded:
        token = manifest_writer_token(mk)
        if token:
            owners.setdefault(token, mk)

    # Orphans: on the store, listed by no manifest.
    assigned, unassigned = defaultdict(list), []
    for base, key in key_by_base.items():
        if base in referenced_bases:
            continue
        token = parquet_writer_token(base)
        if token and token in owners:
            assigned[owners[token]].append(key)
        else:
            unassigned.append(key)

    plans = {}
    for mk, (raw, existing) in loaded.items():
        by_base, corrected = {}, 0
        for f in existing:
            base = f.get("file_path", "").rsplit("/", 1)[-1]
            if base in key_by_base:
                canonical = path_prefix + key_by_base[base]
                if f.get("file_path") != canonical:
                    f = {**f, "file_path": canonical}
                    corrected += 1
            by_base[base] = f

        added = 0
        for key in assigned.get(mk, []):
            base = key.rsplit("/", 1)[-1]
            if base in by_base:
                continue
            try:
                by_base[base] = build_file_entry(store, key, path_prefix)
                added += 1
            except Exception as exc:
                print(f"    skipped {base}: {exc}")

        files = [by_base[b] for b in sorted(by_base)]
        plans[path_prefix + mk] = {
            "key": mk,
            "raw": raw,
            "files": files,
            "added": added,
            "corrected": corrected,
            "events_ingested": sum(f["num_rows"] for f in files),
            "storage_size": sum(f["file_size"] for f in files),
            "file_count": len(files),
        }
    return plans, unassigned


# ------------------------------------------------------------------------------ main


def load_env_file(path):
    """Parse a Parseable-style KEY=VALUE env file. Ignores blanks, comments and `export`."""
    values = {}
    with open(path) as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.removeprefix("export ").partition("=")
            val = val.strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
                val = val[1:-1]
            values[key.strip()] = val
    return values


def resolve_credentials(backend, extra, endpoint_arg):
    """Build the child-process environment and endpoint. Secrets stay in env, never argv."""
    env = os.environ.copy()
    env.update(extra)

    endpoint = endpoint_arg or env.get("P_S3_URL") or None
    if backend != "s3":
        return env, None

    # Map Parseable's variables onto the AWS ones, without overriding an explicit AWS_* or
    # a configured profile.
    if not env.get("AWS_ACCESS_KEY_ID") and env.get("P_S3_ACCESS_KEY"):
        env["AWS_ACCESS_KEY_ID"] = env["P_S3_ACCESS_KEY"]
    if not env.get("AWS_SECRET_ACCESS_KEY") and env.get("P_S3_SECRET_KEY"):
        env["AWS_SECRET_ACCESS_KEY"] = env["P_S3_SECRET_KEY"]
    if not env.get("AWS_DEFAULT_REGION") and env.get("P_S3_REGION"):
        env["AWS_DEFAULT_REGION"] = env["P_S3_REGION"]
    return env, endpoint


def describe_credentials(backend, env, endpoint):
    """One line saying where credentials came from. Never prints a secret."""
    if backend == "local":
        return "local filesystem"
    if backend == "gcs":
        adc = env.get("GOOGLE_APPLICATION_CREDENTIALS")
        return f"gcs via {'GOOGLE_APPLICATION_CREDENTIALS' if adc else 'gsutil/ADC defaults'}"
    if env.get("AWS_ACCESS_KEY_ID"):
        src = "P_S3_ACCESS_KEY" if env.get("P_S3_ACCESS_KEY") else "AWS_ACCESS_KEY_ID"
        who = f"key {env['AWS_ACCESS_KEY_ID'][:4]}... from {src}"
    else:
        who = f"aws profile {env.get('AWS_PROFILE', 'default')} / instance role"
    return f"s3 via {who}" + (f", endpoint {endpoint}" if endpoint else "")


def find_stream_jsons(store, stream_filter):
    """<stream>/.stream/.stream.json and <stream>/.stream/.ingestor.{id}.stream.json"""
    pat = re.compile(r"^(?P<stream>[^/]+)/\.stream/\.(?:ingestor\.[^/]+)?\.?stream\.json$")
    found = []
    for key in store.ls(""):
        m = pat.match(key)
        if m and (not stream_filter or m.group("stream") == stream_filter):
            found.append((m.group("stream"), key))
    return sorted(set(found))


def process(store, stream, key, meta, args, today, multi_writer, stamp):
    """Repair one stream.json (and, with --rebuild, its manifests). Returns True if anything
    was changed or would be changed."""
    old = meta.get("snapshot", {}).get("manifest_list", [])
    if not old:
        return False

    new, notes = collapse_snapshot(old)
    duplicated = len(new) != len(old)

    if not duplicated and not args.check_orphans and not args.rebuild:
        print(f"{key}: {len(old)} entries, clean")
        return False

    print(f"\n{key}")
    if duplicated:
        before, after = totals(old), totals(new)
        print(f"  entries      {len(old)} -> {len(new)}")
        for m in METRICS:
            ratio = f"  ({before[m] / after[m]:.1f}x)" if after[m] else ""
            print(f"  {m:<16} {before[m]:>14,} -> {after[m]:>14,}{ratio}")
        for n in notes:
            print(n)
    else:
        print(f"  entries      {len(old)}, no duplication")

    dates = sorted({d for d in (date_of(e) for e in old) if d})
    if not (args.check_orphans or args.rebuild):
        if args.apply:
            write_snapshot(store, key, meta, new, stamp)
        return duplicated

    # Per-date audit, and (with --rebuild) plan manifest repairs. Plans are computed first and
    # written only after backups are taken, so nothing is mutated before it is backed up.
    order, buckets = group_by_date(new)
    bucket_key = {k[0][:10]: k for k in order if isinstance(k[0], str)}
    date_plans = {}
    print()
    for date in dates:
        parquet, manifests = scan_date(store, stream, date)
        referenced = manifest_referenced_bases(store, manifests)
        names = {k.rsplit("/", 1)[-1] for k in parquet}
        orphaned = names - referenced
        mark = "  <-- ORPHANED" if orphaned else ""
        print(
            f"  {date}: {len(names)} parquet, {len(names & referenced)} referenced, "
            f"{len(orphaned)} unreferenced{mark}"
        )

        if not args.rebuild:
            continue
        if multi_writer:
            print("    SKIPPED: stream has multiple stream.json (distributed) - see docstring")
            continue
        if date == today and not args.force_today:
            print("    SKIPPED: today's partition is live - stop ingestion and use --force-today")
            continue

        prefix = infer_path_prefix(buckets.get(bucket_key.get(date), []), manifests)
        if prefix is None:
            print("    SKIPPED: no manifest_path in the snapshot matches an object on the store")
            continue

        plans, unassigned = plan_rebuild_date(store, stream, date, parquet, manifests, prefix)
        touched = {p: s for p, s in plans.items() if s["added"] or s["corrected"]}
        if not touched and not unassigned:
            print("    nothing to rebuild")
            continue

        date_plans[date] = plans
        verb = "restored" if args.apply else "would restore"
        for mpath, s in sorted(touched.items()):
            bits = []
            if s["added"]:
                bits.append(f"added {s['added']} orphan(s)")
            if s["corrected"]:
                bits.append(f"fixed {s['corrected']} path(s)")
            print(
                f"    {verb} {mpath.rsplit('/', 1)[-1]}: {', '.join(bits)} "
                f"-> {s['file_count']} files, {s['events_ingested']:,} rows"
            )
        if unassigned:
            print(
                f"    ! {len(unassigned)} parquet match no existing manifest and were left "
                "orphaned - their writer's manifest is gone; rebuild needs a Rust admin command"
            )

    # Apply manifest writes (each backed up first), then fold the recovered per-writer stats
    # back into the snapshot, preserving entries for writers with nothing to change.
    if date_plans:
        if args.apply:
            for plans in date_plans.values():
                for s in plans.values():
                    if s["added"] or s["corrected"]:
                        store.write(f"{s['key']}.bak-{stamp}", s["raw"])
                        store.write(
                            s["key"],
                            encode_manifest({"version": MANIFEST_VERSION, "files": s["files"]}),
                        )
        new = merge_rebuilt_snapshot(order, buckets, date_plans)

    if args.apply and (duplicated or date_plans):
        write_snapshot(store, key, meta, new, stamp)
    return duplicated or bool(date_plans)


def merge_rebuilt_snapshot(order, buckets, date_plans):
    """Fold rebuilt per-manifest stats into the collapsed snapshot. Events and storage come
    from the parquet (ground truth); ingestion size is JSON bytes and survives only in the
    snapshot, so it is carried over per manifest_path. Entries whose manifest was not part of
    a plan are kept unchanged; a plan for a manifest with no prior entry is appended."""
    final = []
    for lower, upper in order:
        date = lower[:10] if isinstance(lower, str) else None
        plans = date_plans.get(date)
        entries = buckets[(lower, upper)]
        if not plans:
            final.extend(entries)
            continue

        seen = set()
        for e in entries:
            s = plans.get(e["manifest_path"])
            if s is None:
                final.append(e)
                continue
            seen.add(e["manifest_path"])
            final.append(
                {
                    **e,
                    "events_ingested": s["events_ingested"],
                    "storage_size": s["storage_size"],
                }
            )
        for mpath, s in sorted(plans.items()):
            if mpath in seen:
                continue
            final.append(
                {
                    "manifest_path": mpath,
                    "time_lower_bound": lower,
                    "time_upper_bound": upper,
                    "events_ingested": s["events_ingested"],
                    "ingestion_size": 0,
                    "storage_size": s["storage_size"],
                }
            )

        rebuilt_rows = sum(s["events_ingested"] for s in plans.values())
        snapshot_rows = totals(entries)["events_ingested"]
        if rebuilt_rows < snapshot_rows:
            print(
                f"  {date}: parquet holds {rebuilt_rows:,} rows vs snapshot {snapshot_rows:,} "
                "- the shortfall is data physically gone, not merely unreferenced"
            )
    return final


def write_snapshot(store, key, meta, new_list, stamp):
    """Back up the current stream.json, then write it with the repaired manifest_list."""
    store.write(f"{key}.bak-{stamp}", store.read(key))
    meta["snapshot"]["manifest_list"] = new_list
    store.write(key, json.dumps(meta).encode())
    print(f"  written; backup at {key}.bak-{stamp}")


def main():
    ap = argparse.ArgumentParser(description="Repair issue #1739 manifest duplication.")
    ap.add_argument("--backend", required=True, choices=["gcs", "s3", "local"])
    ap.add_argument("--bucket", help="bucket name (gcs/s3)")
    ap.add_argument("--root", help="store root (local)")
    ap.add_argument("--prefix", default="", help="tenant / path prefix within the store")
    ap.add_argument("--stream", help="limit to one stream")
    ap.add_argument("--check-orphans", action="store_true", help="audit parquet references")
    ap.add_argument("--rebuild", action="store_true", help="also rebuild manifests (Tier 2)")
    ap.add_argument("--force-today", action="store_true", help="rebuild today's partition too")
    ap.add_argument("--apply", action="store_true", help="write repairs back (default: dry run)")
    ap.add_argument("--env-file", help="Parseable-style KEY=VALUE file to source credentials from")
    ap.add_argument("--endpoint-url", help="S3-compatible endpoint (MinIO, Ceph, R2)")
    args = ap.parse_args()

    if args.rebuild:
        missing = [m for m in ("pyarrow", "zstandard") if not _importable(m)]
        if missing:
            sys.exit(f"--rebuild needs: pip install {' '.join(missing)}")
        args.check_orphans = True

    extra = {}
    if args.env_file:
        try:
            extra = load_env_file(args.env_file)
        except OSError as exc:
            sys.exit(f"--env-file: {exc}")

    env, endpoint = resolve_credentials(args.backend, extra, args.endpoint_url)
    print(f"auth: {describe_credentials(args.backend, env, endpoint)}\n")

    store = Storage(args.backend, args.bucket, args.root, args.prefix, endpoint, env)

    targets = find_stream_jsons(store, args.stream)
    if not targets:
        sys.exit("no stream.json found - check --bucket/--root, --prefix and --stream")

    writers = defaultdict(int)
    for stream, _ in targets:
        writers[stream] += 1

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    today = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    affected = 0

    for stream, key in targets:
        try:
            meta = json.loads(store.read(key))
        except Exception as exc:
            print(f"{key}: unreadable: {exc}")
            continue

        # process() owns all writes (manifests and stream.json) so backups always precede the
        # object they protect.
        if process(store, stream, key, meta, args, today, writers[stream] > 1, stamp):
            affected += 1

    print()
    if not affected:
        print("nothing to repair")
    elif args.apply:
        print(f"repaired {affected} snapshot(s)")
        print("restart affected nodes so they reload the corrected per-date counters")
    else:
        print(f"{affected} snapshot(s) need repair - re-run with --apply to write")
        print("stop ingestion to these streams first, or a running node will overwrite the fix")


def _importable(name):
    try:
        __import__(name)
        return True
    except ImportError:
        return False


if __name__ == "__main__":
    main()
