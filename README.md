# cf-bsky-bronze — Bluesky Jetstream to S3 bronze parquet writer

A long-running service that connects to a Bluesky Jetstream relay over WebSocket, normalises the event stream into a fixed five-column schema, and writes snappy-compressed parquet files to S3-compatible object storage. Designed to run containerised on any host with Docker Compose and an S3 bucket.

## Prerequisites

- **Docker and Docker Compose** (Compose v2, Docker Engine 28+ recommended).
- **S3-compatible object storage** with a writable bucket and an access key / secret key pair. Hetzner Object Storage, MinIO, or any S3-API-compatible service works.
- **A Jetstream relay URL.** The public Bluesky relays (`wss://jetstream2.us-east.bsky.network`) are a reasonable default; any spec-compliant relay works.

## Quickstart

1. **Clone the repository.**
   ```bash
   git clone https://github.com/scott-clark-foundry/cf-bsky-bronze.git
   cd cf-bsky-bronze
   ```

2. **Create your `.env` file.**
   ```bash
   cp .env.example .env
   ```

3. **Fill in the required values** in `.env`:
   ```bash
   HETZNER_S3_ENDPOINT=https://your-region.your-objectstorage.example
   HETZNER_S3_ACCESS_KEY=your-access-key
   HETZNER_S3_SECRET_KEY=your-secret-key
   HETZNER_S3_BUCKET=your-bucket-name
   BRONZE_S3_PREFIX=bronze/
   JETSTREAM_URL=wss://jetstream2.us-east.bsky.network
   ```

4. **Build and start the container.**
   ```bash
   docker compose up -d
   ```

5. **Check the logs.** Within a few seconds you should see `ingest_start` followed by `ws_connected` and then repeated `batch_full` or `batch_written` events.
   ```bash
   docker logs bronze --tail 50
   ```

6. **Confirm parquet files are appearing in your bucket.** Either use your provider's web console or run the status probe:
   ```bash
   docker exec bronze python -m bluesky_ingest.s3_status
   # Prints one tab-separated line:  <file_count>\t<newest_filename>
   ```

7. **Verify the cursor recovery path** by stopping and restarting the container. The service will list the prefix on startup, parse the maximum filename integer, and resume from exactly where it left off — no separate cursor file is needed.
   ```bash
   docker compose down && docker compose up -d
   docker logs bronze --tail 20 | grep -E 'cursor_resumed|cold_start'
   ```

## Configuration reference

All configuration is read from environment variables. Pass them via the `.env` file that `compose.yaml` picks up automatically.

### Required

| Variable | Description |
|---|---|
| `HETZNER_S3_ENDPOINT` | Full endpoint URL including scheme, e.g. `https://nbg1.your-objectstorage.com` |
| `HETZNER_S3_ACCESS_KEY` | S3 access key |
| `HETZNER_S3_SECRET_KEY` | S3 secret key |
| `HETZNER_S3_BUCKET` | Bucket name (no `s3://` prefix) |
| `BRONZE_S3_PREFIX` | Object prefix for bronze files, e.g. `bronze/`. Must end with `/`. The writer also uses `<prefix>-tmp/` for in-flight staging. |
| `JETSTREAM_URL` | Jetstream relay base URL, e.g. `wss://jetstream2.us-east.bsky.network` |

### Optional (defaults shown)

| Variable | Default | Description |
|---|---|---|
| `INGEST_BATCH_SIZE` | `100000` | Events per parquet file. Smaller values produce more files and smoother memory use; larger values reduce S3 PUT overhead. |
| `INGEST_TIMEOUT_SECONDS` | `30` | How long to wait for the next WebSocket message before treating the connection as idle and flushing the current batch. |
| `BLUESKY_INGEST_SLEEP_SECONDS` | `60` | Sleep between iterations of the outer `main_loop`. Each iteration re-derives the cursor from S3 LIST. |

(The writer reads only the variables above. The `bronze-status` helper has its own optional env vars — see the next section.)

## bronze-status install (optional operator helper)

`bin/bronze-status` is a bash script that samples container metrics (`docker stats`, `docker inspect`) plus S3 state (via `docker exec <container> python -m bluesky_ingest.s3_status`) and emits one JSONL line per invocation. It is wrapped by a systemd one-shot timer that fires 60 seconds after boot and then every 600 seconds (`OnBootSec=60s`, `OnUnitActiveSec=600s`).

**Environment variables** (read by the helper, not by the writer):

| Variable | Default | Description |
|---|---|---|
| `BRONZE_CONTAINER_NAME` | `bronze` | Container name the helper targets for `docker stats` / `docker exec` / `docker logs`. |
| `BRONZE_STATUS_LOG` | `/var/log/bronze-status.jsonl` | Path the helper appends its JSONL samples to (in addition to stdout). |

**Install steps:**

1. Copy the unit files to systemd's system directory:
   ```bash
   sudo cp systemd/bronze-status.service systemd/bronze-status.timer \
       /etc/systemd/system/
   ```

2. Edit the placeholder paths in the service unit to point to your clone:
   ```bash
   sudo systemctl edit bronze-status.service --force
   ```
   Override `ExecStart=` and `EnvironmentFile=` to match your clone location (the defaults assume `/opt/cf-bsky-bronze/`):
   ```ini
   [Service]
   ExecStart=
   ExecStart=/path/to/cf-bsky-bronze/bin/bronze-status
   EnvironmentFile=
   EnvironmentFile=/path/to/cf-bsky-bronze/.env
   ```

3. Reload and enable:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now bronze-status.timer
   ```

4. Verify the timer is scheduled:
   ```bash
   systemctl list-timers bronze-status.timer
   ```

**Changing the cadence** (e.g., for a soak or debug session at 60-second sampling):
```bash
sudo systemctl edit bronze-status.timer
```
Add the override:
```ini
[Timer]
OnUnitActiveSec=60s
```
`sudo systemctl revert bronze-status.timer` removes the override and restores the default 600-second interval.

**JSONL fields emitted per invocation:**

| Field | Type | Description |
|---|---|---|
| `ts` | string | UTC timestamp of the sample |
| `mem_usage_bytes` | integer\|null | Container RSS in bytes; null if container absent |
| `mem_pct` | string\|null | Container memory as % of limit |
| `cpu_pct` | string\|null | Container CPU % at sample time |
| `restart_count` | integer\|null | Docker restart counter |
| `oom_killed` | boolean | True if the container was OOM-killed |
| `s3_file_count` | integer\|null | Count of parquet files currently in the configured prefix |
| `s3_newest_file` | string\|null | Filename of the most recent parquet file |
| `last_batch_ts` | string\|null | Timestamp of the last `batch_full` or `batch_written` log event in the past 24 h |
| `batch_count_60s` | integer | Batch flush events in the last 60 s |
| `s3_put_failed_count_60s` | integer | `s3_put_failed` log events in the last 60 s |

## Logging

**Format:** structlog JSON lines (one JSON object per line on stdout)
**Destination:** stdout only
**Rotation:** none — rely on the runtime's log capture (docker logs / journald)

The writer is explicitly stdout-only. Docker captures stdout and makes it available via `docker logs`; systemd captures it in journald if you wrap the container with a systemd unit. A file handler was intentionally not added: a non-root container process (UID 10001) cannot reliably write to host paths, and writing into the container filesystem would be lost on container restart. Stdout is the universal, operationally safe channel.

**How to inspect:**
- `docker logs bronze --tail 100`
- `docker logs bronze -f`
- `bin/bronze-status` (one-shot operator probe; emits a JSONL line summarising container + S3 + log state)

**Notable events:**

- `ingest_start` — process entry; fields: `batch_size`, `timeout_s`, `jetstream_url`, `s3_bucket`, `bronze_s3_prefix`, `sleep_seconds`, `max_run_seconds`
- `cold_start_no_cursor` — empty prefix at startup; resumed from Jetstream live tail
- `cursor_resumed_from_list` — cursor derived from S3 LIST; field: `time_us` (the resolved value)
- `ws_connected` — WebSocket opened; field: `cursor`
- `batch_full` — batch flushed because it filled to `INGEST_BATCH_SIZE`. Fields: `events`, `cursor`, `file_name`, `lag_us`. NOTE: at firehose rate every batch fills, so this event alone does NOT mean catchup. Read `lag_us` (= now_us − cursor_us) to distinguish: tens of milliseconds = at live tail; seconds-or-more = catchup.
- `batch_written` — batch flushed at a natural boundary (WS idle, server-side close, or `MAX_RUN_SECONDS` ceiling). Fields: `events`, `cursor`, `file_name`, `lag_us`, plus `batch_write_duration_ms` and `closed_early` (true if a server-side close triggered the flush mid-batch).
- `s3_put_failed` — PUT or rename failure; loop continues, error logged. Field: `error`.
- `iter_crashed` — non-S3 error in the iter; loop continues. Field: `error`.
- `bronze_tmp_sweep_failed` — startup tmp sweep encountered an error; non-fatal, ingest proceeds. Field: `error`.
- `ingest_crashed` — fatal error caught in `main()`; process exits non-zero. Field: `error`.
- `ws_closed_by_server` / `ws_idle_timeout` / `bronze_tmp_orphans_swept` / `run_complete` / `ingest_end` — additional lifecycle events; fields self-evident from the JSON output.

## Operational notes

**Restart policy.** `compose.yaml` sets `restart: unless-stopped`, so Docker restarts the container automatically after an unhandled crash or host reboot. You do not need a systemd unit wrapping the container for liveness.

**Crash recovery.** The cursor is never stored on disk. On every startup `run_once` issues a paginated S3 LIST of `<BRONZE_S3_PREFIX>`, parses all filenames as integers, takes the maximum, and resumes from `max + 1`. A clean restart mid-batch loses only the in-flight events since the last committed file — the next run picks up from the last successfully written filename.

**Half-written batches.** The writer stages in-progress parquet files under `<BRONZE_S3_PREFIX>-tmp/` (e.g. `bronze-tmp/`). A crash before `finalize()` leaves a temporary object there. On the next startup `cleanup_orphaned_tmp` sweeps any tmp objects older than one hour, so stale partials are automatically removed.

**Memory.** `compose.yaml` sets `mem_limit: 1g`. The streaming writer (class `StreamingBronzeWriter`) flushes to S3 in 10,000-row chunks regardless of batch size, so peak memory is roughly constant across large catchup batches. If you see OOM kills (`oom_killed: true` in `bronze-status` output), raise `mem_limit` in `compose.yaml` or reduce `INGEST_BATCH_SIZE`.

## References

- **Bluesky Jetstream:** <https://github.com/bluesky-social/jetstream>
- **AT Protocol overview:** <https://atproto.com/guides/overview>
- **structlog:** <https://www.structlog.org/>
- **DuckDB** (a natural downstream consumer for the bronze parquet files): <https://duckdb.org/>
- **Hetzner Object Storage:** <https://docs.hetzner.com/storage/object-storage/>
