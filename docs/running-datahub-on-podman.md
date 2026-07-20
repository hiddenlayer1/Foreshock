# Running DataHub Core on Podman

DataHub's quickstart targets Docker. It does run on Podman, but three things bite
on the CLI path, and two more if you fall back to driving compose yourself. This
is the exact path that worked, recorded so the stack can be brought up from
scratch without rediscovering them.

Verified 2026-07-18 against `acryl-datahub` 1.6.0.15 and DataHub `v1.5.0.6`,
Podman 5.8.2 on a WSL2 machine. Compose-fallback path re-verified 2026-07-19
from a fully stopped stack.

## 1. Point the Docker API at Podman

The DataHub CLI drives containers through the `docker` Python SDK, which honours
`DOCKER_HOST`. If a Docker Desktop CLI is on `PATH` with no daemon behind it, the
quickstart fails with *"check if the daemon is running"*.

```bash
export DOCKER_HOST='npipe:////./pipe/podman-machine-default'   # Windows
# export DOCKER_HOST="unix://$XDG_RUNTIME_DIR/podman/podman.sock"   # Linux
```

Confirm it is routed before going further — this should list your Podman containers:

```bash
docker ps
docker compose version
```

## 2. Pre-create the bind-mount source directories

**This is the one that actually blocks the stack.** Docker silently creates a
missing bind-mount source directory; Podman refuses and the GMS container fails
to create:

```
Error response from daemon: container create:
  statfs /mnt/c/Users/<you>/.datahub/search: no such file or directory
```

The compose file bind-mounts four host paths. Create them first:

```bash
mkdir -p ~/.datahub/search ~/.datahub/plugins ~/.aws/sso/cache
```

(`~/.datahub/plugins` is created by the CLI; `~/.datahub/search` and the `~/.aws`
paths are not.)

## 3. Give compose the variables the CLI normally injects

If you fall back to driving `docker compose` directly — see below — the compose
file needs variables the `datahub` CLI would otherwise supply. Without
`DATAHUB_VERSION` the image reference resolves to `acryldata/datahub-upgrade:`
with an empty tag and compose rejects it as an invalid reference.

```bash
cd ~/.datahub/quickstart
set -a; source ./.local-secrets.env; set +a    # signing key + salt
export DATAHUB_VERSION="v1.5.0.6"
export UI_INGESTION_DEFAULT_CLI_VERSION="1.5.0.6"
```

## Bringing it up

```bash
pip install acryl-datahub
export DOCKER_HOST='npipe:////./pipe/podman-machine-default'
mkdir -p ~/.datahub/search ~/.datahub/plugins ~/.aws/sso/cache
datahub docker quickstart
```

If the CLI creates the containers but leaves them in `Created` without starting
them, drive the compose file directly with the variables from step 3 — plus the
two below, both of which fail quietly rather than loudly.

### 4. `--profile quickstart`, or you get Kafka and nothing else

Every service except `kafka-broker` sits behind the `quickstart` compose profile.
Without the flag, `up -d` starts the broker, prints no warning, and **exits 0** —
so it reads as success right up until GMS is not there.

### 5. `HOME` must be set if you are on Windows

The compose file uses `${HOME}` as the bind-mount source for `.datahub/plugins`,
`.datahub/search` and `.aws`. PowerShell has no `HOME` — it is `USERPROFILE` —
so the sources resolve to `/.datahub/search` and Podman fails them with exactly
the `statfs ... no such file or directory` from step 2. Compose warns
`The "HOME" variable is not set`, which is easy to scroll past.

```bash
export HOME="${HOME:?}"   # already set on Linux/macOS; no-op there
docker compose -p datahub -f ~/.datahub/quickstart/docker-compose.yml \
  --profile quickstart up -d
```

PowerShell, where both traps actually bite:

```powershell
$env:DOCKER_HOST = 'npipe:////./pipe/podman-machine-default'
$env:HOME        = $env:USERPROFILE
$env:DATAHUB_VERSION = 'v1.5.0.6'
$env:UI_INGESTION_DEFAULT_CLI_VERSION = '1.5.0.6'
Get-Content "$env:USERPROFILE\.datahub\quickstart\.local-secrets.env" |
  ForEach-Object {
    if ($_ -match '^\s*([^#=]+)=(.*)$') {
      [Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim())
    }
  }
docker compose -p datahub -f "$env:USERPROFILE\.datahub\quickstart\docker-compose.yml" `
  --profile quickstart up -d
```

Startup is ordered: OpenSearch, Kafka and MySQL must report healthy before
`system-update` runs, and GMS only starts once that job has exited 0. Allow a
few minutes from cold.

## Verifying

```bash
curl -o /dev/null -w '%{http_code}\n' http://localhost:8080/health   # GMS  -> 200
curl -o /dev/null -w '%{http_code}\n' http://localhost:9002          # UI   -> 200
curl -s http://localhost:9200/_cluster/health                        # -> yellow or green
```

**Check OpenSearch explicitly. The first two checks pass without it.** GMS keeps
answering `/health` with 200 and the UI keeps serving after OpenSearch has died,
so the stack looks up by the two obvious probes while every search and lineage
query fails underneath. Observed 2026-07-19: the only visible symptom was the
demo failing on a GraphQL search error with both HTTP checks green.

`yellow` is correct on a single node — replica shards have nowhere to go. `red`
or a refused connection means it is down:

```bash
docker start datahub-opensearch-1
```

If it exits 127 shortly after starting, that is the JVM dying, not a missing
command — check the logs for
`OutOfMemoryError: unable to create native thread`. OpenSearch wants headroom
the machine may not have if other containers are running. Free memory elsewhere
rather than resizing the Podman machine, which stops it and bounces every other
container on the host.

A healthy stack looks like this:

| Container | Expected |
|---|---|
| `datahub-datahub-gms-quickstart-1` | Up (healthy) |
| `datahub-frontend-quickstart-1` | Up |
| `datahub-kafka-broker-1` | Up (healthy) |
| `datahub-opensearch-1` | Up (healthy) |
| `datahub-mysql-1` | Up (healthy) |
| `datahub-system-update-quickstart-1` | Exited (0) — a migration job, exiting 0 is success |

## When search breaks but the stack still reports healthy

The failure worth knowing about, because nothing in the health table catches it.

OpenSearch can die on its own hours after a clean start, and **GMS keeps
reporting healthy without it**. `/health` returns 200, the UI loads, and the
containers look fine. What breaks is every search-backed GraphQL query:

```
Failed to execute search: entity types [DATASET], query *, ...
  extensions: { code: 500, type: SERVER_ERROR }
```

Anything that resolves entities by search fails; anything that reads an aspect
by URN still works. That split is the tell.

Confirm it in one call:

```bash
docker ps -a --filter name=opensearch      # Exited (127) (unhealthy)
curl -s http://localhost:9200/_cluster/health
```

The container log ends in `pthread_create failed (EAGAIN)` and
`java.lang.OutOfMemoryError: unable to create native thread`. That reads as a
thread-limit problem and is not one — `/proc/sys/kernel/threads-max` is six
figures and nowhere near reached. It is memory: each thread stack needs about a
megabyte of *free* memory, and when the machine has only a few hundred
megabytes genuinely free the JVM cannot get one. `OOMKilled` stays `false`
because nothing was killed by the cgroup — the allocation simply failed.

Recovery is just a restart; the indices are on a volume and survive:

```bash
docker start datahub-opensearch-1
curl -s http://localhost:9200/_cluster/health    # red -> yellow, ~1 min
```

Wait for `red` to clear before using the stack. `red` means shards are still
recovering and search will keep failing; `yellow` is the healthy steady state
for a single node, because replica shards have nowhere to go and stay
unassigned by design. Do not chase `green` here.

To stop it recurring, give the machine memory rather than restarting on a
schedule — DataHub's JVMs plus whatever else shares the machine is what
exhausts it. `free -h` inside the machine is the number that matters.

## Ports

`9002` UI · `8080` GMS · `9092` Kafka · `3306` MySQL · `9200` OpenSearch.
Check these are free before starting; the quickstart offers `--mysql-port`,
`--kafka-broker-port` and `--elastic-port` if they are not.

## Resources

Roughly 2 CPU / 8 GB RAM / 13 GB disk. On WSL2, memory is governed by
`.wslconfig` on the Windows host, **not** by `podman machine set`. `podman machine
list` may report a small nominal figure while the machine actually has far more —
check `podman machine ssh free -h` before changing anything, since resizing the
machine requires stopping it and will bounce every other container on the host.
