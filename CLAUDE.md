# CLAUDE.md — cf-bsky-bronze

## Purpose

cf-bsky-bronze is a stateless Bluesky Jetstream to S3 bronze parquet writer. It connects to a Jetstream WebSocket relay, drains `commit`, `identity`, and `account` events, normalises each to a fixed five-column schema (`data, kind, time_us, created_ts, dedup_hash`), and writes snappy-compressed parquet files to S3-compatible object storage. The headline architectural choice is **cursor-from-LIST**: there is no on-disk cursor file or database. On every startup the service lists the configured S3 prefix, parses all `<integer>.parquet` filenames, takes the maximum, and resumes from `max + 1`. Downstream consumers derive the same cursor by the same listing rule.

## Where things live

- **`src/bluesky_ingest/main.py`** — entry point (`main`), outer retry loop (`main_loop`), single ingest iteration (`run_once`). Contains all log event names and their field sets.
- **`src/bluesky_ingest/config.py`** — `Config` dataclass + `load_config()`. All env var names live here; there is no TOML config, no on-disk cursor, no database.
- **`src/bluesky_ingest/parquet_writer.py`** — `BRONZE_SCHEMA` (the cross-component contract), `StreamingBronzeWriter` (the production writer), and the in-memory helpers `build_table` / `write_parquet_bytes` used by tests.
- **`src/bluesky_ingest/cursor_from_list.py`** — `resolve_cursor_us`: paginates S3 LIST, parses filenames as integers, returns the maximum or `None` for an empty prefix.
- **`src/bluesky_ingest/s3_status.py`** — invoked by `bronze-status` via `docker exec <container> python -m bluesky_ingest.s3_status`; prints `<count>\t<newest_filename>` without requiring S3 credentials on the host.
- **`src/bluesky_ingest/extract.py`** — `extract_row`: JSON parse + field extraction for a single Jetstream message.
- **`src/bluesky_ingest/logging.py`** — `setup_logging`: configures structlog for JSON-to-stdout output. No file handler.
- **`src/bluesky_ingest/s3.py`** — `make_s3_client`: constructs the boto3 client from `Config`.
- **`bin/bronze-status`** — bash operator helper. Samples `docker stats`, `docker inspect`, and `s3_status` via `docker exec`; emits one JSONL line per invocation to stdout and appends to `${BRONZE_STATUS_LOG}`.
- **`systemd/bronze-status.service`** and **`systemd/bronze-status.timer`** — one-shot systemd timer wrapping `bin/bronze-status`; first sample 60 s after boot, then every 600 s (`OnBootSec=60s`, `OnUnitActiveSec=600s`). Placeholder paths assume `/opt/cf-bsky-bronze/`; operators edit to match their clone path.
- **`Dockerfile`** — `python:3.12-slim`, uv-managed deps. The container user is named `bluesky` (UID 10001); the script entrypoint is `bluesky-ingest` — easy to confuse, but distinct.
- **`compose.yaml`** — single `bronze` service, `restart: unless-stopped`, `mem_limit: 1g`.
- **`.env.example`** — public template for the required and optional env vars. `.env` is gitignored.
- **`tests/`** — pytest suite; uses `moto[s3]` for S3 fakes. Run with `uv run pytest`.

## Load-bearing invariants

These are constraints a future Claude (or contributor) could break without immediately obvious failures. Think carefully before touching any of them.

1. **`BRONZE_SCHEMA` is a cross-component contract.** Downstream consumers read parquet files from `<BRONZE_S3_PREFIX><last_time_us>.parquet` and expect exactly five fields: `data` (string), `kind` (string), `time_us` (int64), `created_ts` (timestamp\[us, UTC\], nullable), `dedup_hash` (string). Changing field names, types, or nullability is a breaking change. `parquet_writer.py` is the authoritative definition; keep `build_table`, `StreamingBronzeWriter`, and any test fixtures in sync.

2. **Filename = cursor.** Files are written as `<BRONZE_S3_PREFIX><last_time_us>.parquet` where `last_time_us` is a monotonically increasing integer (Jetstream µs since epoch). Lex sort equals numeric sort equals time order. `cursor_from_list.resolve_cursor_us` and `s3_status` both parse filenames as bare integers. Do not introduce subdirectories, date-path segments, or non-integer components in the final key — either module would silently skip them, causing the cursor to rewind or the file count to be wrong.

3. **Successful S3 PUT = commit.** There is no separate cursor file, database, or sidecar store. The only crash-recovery mechanism is re-deriving the cursor from S3 LIST. Any change that adds a secondary cursor store creates split-brain: the two stores can disagree after a partial write, and there is no reconciliation path. Don't do it.

4. **Stdout-only logging.** `logging.py` configures structlog to write JSON lines to stdout only. Do NOT re-add a file handler. A non-root container process (UID 10001) cannot reliably open host paths, and writing into the container filesystem is lost on restart. If you see pressure to persist logs, the operator-facing answer is `docker logs` / journald capture, not a file handler inside the process.

5. **`<BRONZE_S3_PREFIX>-tmp/` orphans are swept at startup, not mid-run.** `cleanup_orphaned_tmp` runs at the top of `run_once`, before cursor resolution. Adding mid-run sweeps introduces a race: the sweeper and the active `StreamingBronzeWriter` would both operate on the `*-tmp/` prefix simultaneously. Startup-only is the safe contract; keep it.

6. **`s3_status` and `cursor_from_list` are both filename-parsing modules.** Both depend on `<integer>.parquet` as the filename format. If you change the filename scheme in `parquet_writer.py`, you must update both consumers in the same commit, and must update this file and `README.md` to reflect the new convention.

7. **Image stays uv-managed + non-root.** The `Dockerfile` installs deps via `uv sync --no-dev --frozen` against `uv.lock`. The process runs as `bluesky` UID 10001. Do not switch to pip, do not run as root, do not relax the frozen lockfile — reproducible builds are the point, and root + container is a security regression.

8. **`compose.yaml` has no volumes and no published ports.** The service is intentionally stateless. Volumes would suggest persistent state (there is none). Published ports would suggest an API (there is none). If a feature proposal adds either, stop and think about whether it belongs in this service at all.

## Project posture

- **Stand-alone, public-portfolio-quality.** This is a published repo read by strangers. It must not reference other internal repos, operator-specific hostnames, infrastructure paths, IP addresses, or cross-project conventions. If a change would introduce such a reference, scrub it.
- **Don't pitch CLI frameworks, config-management abstractions, or orchestration layers.** The design is intentionally minimal: Docker Compose + env vars + bash operator scripts. Re-evaluate only if requirements change substantially.
- **Don't add features `README.md` doesn't call for.** New capabilities require a documented operator need first; undocumented features are maintenance surface without value signal.
