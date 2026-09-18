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
| `docker-compose.dev.yaml` | Local development services, ports, bind mounts, and image build |
| `docker-compose.prod.yaml` | Production overrides for an immutable image and externally managed services |
| `.env.dev.example` | Safe development configuration template |
| `.env.prod.example` | Safe production configuration template |
| `scripts/compose.sh` | Selects and runs the requested environment |

### Development

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