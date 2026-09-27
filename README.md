# Lightweight Data Stack: ClickHouse + Airflow + Superset

Everything runs in Docker containers on one shared network (`data-stack`), so each service can reach the others by name — e.g. Airflow and Superset both connect to ClickHouse using the hostname `clickhouse`, not `localhost`.

## What's included

| Service | Purpose | Port (on your machine) |
|---|---|---|
| ClickHouse | Analytics database | 8123 (HTTP), 9000 (native) |
| Airflow (webserver + scheduler) | Orchestration / scheduled jobs | 8080 |
| Airflow Postgres | Metadata DB required by Airflow | (internal only) |
| Superset | BI dashboards / visualization | 8088 |

## 0. Opening this in VS Code

1. Open VS Code
2. `File → Open Folder` → select this `data-stack` folder
3. VS Code will prompt to install recommended extensions (Docker, Python, YAML) — click **Install All**
4. Open the integrated terminal: `` Ctrl + ` ``
5. All commands below are run from that terminal, inside this folder

**Quick actions via VS Code Tasks** (no typing needed):
`Ctrl+Shift+P` → type `Run Task` → choose one of:
- `Stack: Up` — starts everything
- `Stack: Down` — stops everything
- `Stack: Status` — shows container status
- `Stack: Logs (...)` — tails logs for a specific service

The Docker extension also adds a **whale icon** in the sidebar where you can see running containers, images, and volumes visually, and start/stop them by right-clicking.

## 1. Prerequisites

You already have Docker installed. Also make sure `docker compose` (the plugin, no hyphen) works:
```bash
docker compose version
```
If that fails, install the compose plugin:
```bash
sudo apt install docker-compose-plugin
```

## 2. Start the whole stack

From inside this folder:
```bash
cd data-stack
docker compose up -d
```

First run will take a few minutes — it downloads ClickHouse, Postgres, Airflow, and Superset images, then Airflow/Superset run their database migrations and create admin users automatically.

## 3. Check everything is running

```bash
docker compose ps
```

You should see `clickhouse`, `airflow-postgres`, `airflow-webserver`, `airflow-scheduler`, and `superset` all listed as `Up`.

## 4. Access each app

- **Airflow**: http://localhost:8080 — login: `admin` / `admin`
- **Superset**: http://localhost:8088 — login: `admin` / `admin`
- **ClickHouse**: http://localhost:8123 (HTTP API) — or connect via CLI (see below)

## 5. Run the sample Airflow DAG

A sample DAG (`clickhouse_sample_dag.py`) is already placed in `airflow/dags/`. It creates a `sales` table in ClickHouse and inserts two rows.

1. Open Airflow at http://localhost:8080
2. Find `clickhouse_sample_dag` in the DAG list
3. Toggle it **on**, then click the **Trigger DAG** (▶) button
4. Check the task logs — it should print the inserted rows

## 6. Connect Superset to ClickHouse

1. Open Superset at http://localhost:8088
2. Go to **Settings → Database Connections → + Database**
3. Choose **ClickHouse Connect** (or "Other" if not listed, and use the SQLAlchemy URI below)
4. Use this connection string:
   ```
   clickhousedb://default:clickhouse@clickhouse:8123/analytics
   ```
   Note: the host here is `clickhouse` (the Docker service name), **not** `localhost` — because Superset is talking to ClickHouse over the internal Docker network.
5. Test the connection, then Save
6. Go to **SQL Lab** and run:
   ```sql
   SELECT * FROM sales;
   ```
   to confirm you can query the data the DAG inserted.

## 7. Stopping / restarting

```bash
docker compose stop        # stop containers, keep data
docker compose up -d       # start them again
docker compose down        # stop and remove containers (data volumes persist)
docker compose down -v     # stop and WIPE all data (careful — deletes volumes)
```

## 8. Checking logs if something fails

```bash
docker compose logs -f airflow-webserver
docker compose logs -f superset
docker compose logs -f clickhouse
```

## About the .env file

All credentials (ClickHouse, Airflow admin, Superset admin, Postgres) live in `.env`, not hardcoded in `docker-compose.yml`. This is standard practice — `.env` is already in `.gitignore` so it won't get committed if you push this to a repo. Change the passwords in `.env` before using this beyond local practice.

## Notes on "lightweight"

- Uses **LocalExecutor** for Airflow (no separate Celery/Redis worker cluster) — fine for learning/dev use
- Uses **Alpine-based images** for ClickHouse and Postgres to keep image size down
- All three apps share a single Docker network instead of separate manual `docker run --link` commands

## Next steps you might want

- Add more DAGs in `airflow/dags/` for other data pipelines
- Add persistent Superset dashboards (they're saved in the `superset_home` volume automatically)
- Swap `admin/admin` credentials for something secure before using this beyond local practice
