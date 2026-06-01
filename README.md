# Monitoring Project Worker

![Logo](/images/icon.png)

Go monitoring worker for API **protocol 2.0** (monitoring-server ≥ v2.1.0): single binary, configuration from environment and optionally from `.env` next to the process or the binary.

- Server
![Docker Pulls](https://img.shields.io/docker/pulls/akeb/monitoring) ![Docker Image Size](https://img.shields.io/docker/image-size/akeb/monitoring/latest) ![Docker Version](https://img.shields.io/docker/v/akeb/monitoring) ![Docker Stars](https://img.shields.io/docker/stars/akeb/monitoring) ![License](https://img.shields.io/badge/license-AGPLv3-blue)

- Worker
![Docker Pulls](https://img.shields.io/docker/pulls/akeb/monitoring-worker-go) ![Docker Image Size](https://img.shields.io/docker/image-size/akeb/monitoring-worker-go/latest) ![Docker Version](https://img.shields.io/docker/v/akeb/monitoring-worker-go) ![Docker Stars](https://img.shields.io/docker/stars/akeb/monitoring-worker-go) ![License](https://img.shields.io/badge/license-AGPLv3-blue)

Monitoring is a powerful distributed monitoring system with a web interface, built on a "Server-Worker" architecture. The system allows you to monitor the availability and performance of your websites, services, and Docker containers from multiple geographically distributed points.

- [Monitoring website](https://akeb.github.io/monitoring/)
- [Documentation](https://github.com/AKEB/monitoring/wiki)
- [Screenshots](https://github.com/AKEB/monitoring/tree/main/screenshots)

## Build

```bash
go build -buildvcs=false -o monitoring-worker ./cmd/worker/
```

If the repository is not a full git checkout and the build fails on VCS, the `-buildvcs=false` flag is required.

## Run

1. Copy `.env.example` from the PHP worker or set the same variables (at minimum `SERVER_HOST`, `WORKER_KEY_HASH`).
2. Place `.env` in the current directory or next to the executable.
3. `./monitoring-worker`

After a `restart` command from the server, the process exits the loop and prints `Exiting` (same as PHP after `loop()`).

### Behavior on frequent errors (same as PHP)

On `finished with error`, the next run time is set as `update_time = now - repeat_seconds`, so the job becomes available again almost immediately. With a small `LOOP_TIMEOUT` and `DEBUG=true`, logs will show many `Start`/`Finish` lines — this is expected; disable `DEBUG` or increase `LOOP_TIMEOUT` if the noise is a problem.

The outbound queue to the server is **deduplicated by `job_id`**: one current result per job (no hundreds of repeats in a single POST). Sends are **FIFO from the head of the queue** (oldest waiters first); with a large queue, batches are sent per loop tick. On POST failure, the failed batch is returned **to the front**, not the tail. Checks are started in order of **most overdue** (`update_time + repeat_seconds`), not random map iteration.

To **avoid re-running a check on every loop tick** after an error/timeout (as PHP does with `update_time = now - repeat_seconds`), set `IMMEDIATE_ERROR_RETRY=false`: then `update_time = now` and the next run will not be sooner than `repeat_seconds`.

## Environment variables

Names and meaning match `monitoring-worker`: `TZ`, `SERVER_HOST`, `WORKER_KEY_HASH`, `WORKER_THREADS`, `JOBS_GET_TIMEOUT`, `LOOP_TIMEOUT`, `RESPONSE_SEND_TIMEOUT`, `LOGS_WRITE_TIMEOUT`, `PROXY_HOST`, `PROXY_TYPE`, `WORKER_VERSION`, `PROTOCOL_VERSION` (default `2.0` — reduced exchange with the server), `DEBUG`, `CURL_DEBUG`, `DOCKER_DEBUG`, `IMMEDIATE_ERROR_RETRY` (default `true`, same as PHP).

### Protocol 2.0 (required)

`PROTOCOL_VERSION` defaults to `2.0`; values `1.0` and empty string are not accepted. The server returns an error if the protocol version is below 2.0.

- **get/** — only required job fields; Docker PEM via `docker_tls_sync` + `docker_update_time` (worker caches PEM and supplies it if the server did not send `tls_*`).
- **state/** — no `response_body`; Docker — only `container_states`; HTTPS — `cert_expire` for SSL alerts.
- API responses without `server_time` / `server_microtime`.
- **`CURL_DEBUG`** — HTTP logging (method, URL, response code, proxy); **`proxy_type`** as in PHP cURL (HTTP/SOCKS5).
- Restart from server: `data.restart` (button in the worker admin UI on the server).

### `WORKER_VERSION` (same as PHP)

In PHP, requests to `/api/monitoring/get/` and `/api/monitoring/state/` include `worker_version` from **`getenv('WORKER_VERSION')`** (`Config.php`). In the PHP Docker image the same value is passed via `ARG/ENV WORKER_VERSION` and `version.php` is rewritten at build time, but at runtime the environment variable wins.

In Go:

1. if **`WORKER_VERSION`** is set in the environment or `.env` — that value is sent to the API;
2. otherwise — the string from **`-ldflags "-X monitoring-worker-go/internal/buildinfo.Version=..."`** (in GitHub Actions this is the ref name: tag `v1.2.3` or `dev-<short sha>`);
3. otherwise — **`local`**, same as the default in `monitoring-worker/src/version.php`.

A local build without ldflags and without env will send `worker_version: "local"`. To match a release, build with `-ldflags` or set `WORKER_VERSION` in `.env`.

### Docker Engine API debugging

In `.env`, set **`DOCKER_DEBUG=true`** (separate from `DEBUG`): stderr will show lines prefixed with `[docker]` — request URL, TLS/mTLS (PEM presence, `LoadX509KeyPair` errors), proxy, response code, body preview. This mirrors the idea of `DOCKER_DEBUG` in PHP `docker.php`.

**Logging:** normal worker messages only when `DEBUG=true`. Errors always go to stderr. `body` preview in `[http-error]` — with `DEBUG` (monitoring API requests), `CURL_DEBUG` (monitor/exporter), `DOCKER_DEBUG` (Docker Engine). Monitoring API responses with `status != 0` — `[api-error][server]` (`api_error` always, `body` only with `DEBUG`). In Docker: `docker logs <container>`; in compose: `docker compose logs -f worker`.

If the job **`host` is an IP** but the daemon certificate is issued for a **DNS name** (e.g. `*.example.com`), you can pass **`tls_server_name`** in the job JSON — it is used for SNI and name verification, while the Engine API URL is still built from `host` and `port`. An error like `x509: certificate signed by unknown authority` with `tls_ca_file` already loaded usually means an incorrect or incomplete PEM chain (you need the CA/intermediates that actually signed the daemon **server** certificate, not only the client mTLS cert).

## CI and releases

File `.github/workflows/build.yml`:

- on **push** to `main`/`master` and on **pull request** — build and artifact in the workflow run (unchanged);
- on **tag push** like `v1.2.3` — same build plus **GitHub Release** with `monitoring-worker` attached (via [softprops/action-gh-release](https://github.com/softprops/action-gh-release)).

Example of publishing a version:

```bash
git tag v1.0.0
git push origin v1.0.0
```

A normal branch push **does not create a release** — only an Actions artifact.
