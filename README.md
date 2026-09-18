# Portfolio Data Pipeline

A production-oriented data pipeline that incrementally extracts product data
from the Open Food Facts API, writes immutable Parquet batches to S3-compatible
object storage, and loads PostgreSQL staging and dimensional-model tables.

Apache Airflow orchestrates the raw, staging, data-quality, and mart workloads.

## Environment model

The project separates shared Airflow configuration from environment-specific
infrastructure:

| File | Purpose |
|---|---|
| `docker-compose.yaml` | Shared Airflow services and configuration |
| `docker-compose.dev.yaml` | Local services, ports, bind mounts, and image build |
| `docker-compose.prod.yaml` | Immutable image and externally managed services |
| `.env.dev.example` | Safe development configuration template |
| `.env.prod.example` | Safe production configuration template |
| `scripts/compose.sh` | Selects and runs the requested environment |

## Development

Development runs the complete platform locally:

- Airflow
- PostgreSQL for Airflow metadata
- Redis
- PostgreSQL for pipeline data
- MinIO for S3-compatible object storage
- Automatic database-table and object-storage initialization
- Source-code bind mounts for rapid development
- Published development ports bound to `127.0.0.1`

Create the local configuration:

```bash
cp .env.dev.example .env.dev
```

Review `.env.dev`, then validate and start the environment:

```bash
./scripts/compose.sh dev config --quiet
./scripts/compose.sh dev up -d
./scripts/compose.sh dev ps -a
```

Airflow is available at <http://localhost:8080>.

The example Airflow username and password are intentionally insecure
development defaults. Do not reuse them outside the local environment.

Stop the development environment while preserving its data:

```bash
./scripts/compose.sh dev down
```

To also delete the local databases and object-storage data:

```bash
./scripts/compose.sh dev down --volumes
```

> Warning: `--volumes` permanently deletes locally persisted development data.

## Production

Production uses the shared Airflow service definitions but:

- Runs a prebuilt immutable image instead of building locally
- Does not mount source code from the host
- Expects externally managed PostgreSQL, Redis, and S3 services
- Does not publish Airflow or Flower ports directly
- Enables remote logging and failure notifications
- Expects secrets to be injected by the deployment platform

`.env.prod.example` documents the required production variables. Never commit a
populated `.env.prod` file.

Create a local copy and render the merged configuration:

```bash
cp .env.prod.example .env.prod
./scripts/compose.sh prod config --quiet
```

Rendering the configuration does not deploy production. A real deployment also
requires external infrastructure, valid secrets, and a published Airflow image.

## Configuration behavior

`PIPELINE_ENV` controls runtime configuration:

- `dev`: loads `.env.dev`, or the file specified by `ENV_FILE_PATH`
- `prod`: does not load a dotenv file inside Python; required values must
  already exist in the process environment

Production configuration fails fast when required pipeline variables are
missing, the pipeline database points to a known local development host, or
`S3_ENDPOINT_URL` attempts to redirect production storage away from AWS S3.

The committed example files contain placeholders only. `.env.dev` and
`.env