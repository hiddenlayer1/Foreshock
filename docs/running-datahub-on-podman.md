# Running DataHub Core on Podman

DataHub's quickstart targets Docker. It does run on Podman, but three things bite.
This is the exact path that worked, recorded so the stack can be brought up from
scratch without rediscovering them.

Verified 2026-07-18 against `acryl-datahub` 1.6.0.15 and DataHub `v1.5.0.6`,
Podman 5.8.2 on a WSL2 machine.

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
them, drive the compose file directly with the variables from step 3:

```bash
docker compose -p datahub -f ~/.datahub/quickstart/docker-compose.yml up -d
```

## Verifying

```bash
curl -o /dev/null -w '%{http_code}\n' http://localhost:8080/health   # GMS  -> 200
curl -o /dev/null -w '%{http_code}\n' http://localhost:9002          # UI   -> 200
```

A healthy stack looks like this:

| Container | Expected |
|---|---|
| `datahub-datahub-gms-quickstart-1` | Up (healthy) |
| `datahub-frontend-quickstart-1` | Up |
| `datahub-kafka-broker-1` | Up (healthy) |
| `datahub-opensearch-1` | Up (healthy) |
| `datahub-mysql-1` | Up (healthy) |
| `datahub-system-update-quickstart-1` | Exited (0) — a migration job, exiting 0 is success |

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
