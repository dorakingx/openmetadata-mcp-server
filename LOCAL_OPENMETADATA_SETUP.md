# Local OpenMetadata Setup (Docker Compose)

Use this guide to quickly spin up a local OpenMetadata instance with sample data for MCP server testing.

## One-command setup

From the project root, run:

```bash
bash setup_local_ometa.sh
```

## Manual steps (equivalent)

```bash
mkdir -p ometa-docker
cd ometa-docker
wget -O docker-compose.yml https://github.com/open-metadata/OpenMetadata/releases/download/1.5.0-release/docker-compose.yml \
  || curl -fsSL https://github.com/open-metadata/OpenMetadata/releases/download/1.5.0-release/docker-compose.yml -o docker-compose.yml
docker compose up -d
```

## Boot process notes

- Initial startup usually takes **3-5 minutes**.
- During boot, MySQL, Elasticsearch, and OpenMetadata web services initialize.
- OpenMetadata UI: `http://localhost:8585`
- Default credentials:
  - Email: `admin@open-metadata.org`
  - Password: `admin`

## Sample data + token for MCP demo

The official compose stack includes ingestion components that populate sample assets (for example `ecommerce_db`, `users`, and related demo entities).

After sample data is loaded:

1. Open OpenMetadata UI.
2. Navigate to `Settings -> Bots -> ingestion-bot`.
3. Click **Copy Token** to retrieve the JWT token.
4. Paste that token into this repo's `.env`:

```env
OPENMETADATA_HOST=http://localhost:8585
OPENMETADATA_JWT_TOKEN=your-copied-ingestion-bot-token
```

Then run your MCP server normally and Claude can discover and audit the demo tables.
