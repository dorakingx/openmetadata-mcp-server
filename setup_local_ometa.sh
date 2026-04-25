#!/usr/bin/env bash
set -euo pipefail

OMETA_DIR="ometa-docker"
COMPOSE_URL="https://github.com/open-metadata/OpenMetadata/releases/download/1.5.0-release/docker-compose.yml"

echo "==> Preparing local OpenMetadata docker directory"
mkdir -p "${OMETA_DIR}"
cd "${OMETA_DIR}"

echo "==> Downloading docker-compose.yml"
if command -v wget >/dev/null 2>&1; then
  wget -O docker-compose.yml "${COMPOSE_URL}"
elif command -v curl >/dev/null 2>&1; then
  curl -fsSL "${COMPOSE_URL}" -o docker-compose.yml
else
  echo "Error: neither wget nor curl is installed."
  exit 1
fi

echo "==> Starting OpenMetadata stack (detached)"
docker compose up -d

cat <<'EOF'

OpenMetadata containers are starting.
Boot usually takes 3-5 minutes while MySQL, Elasticsearch, and the web service initialize.

UI: http://localhost:8585
Default login:
  - Email: admin@openmetadata.org
  - Password: admin

When sample data appears, get your ingestion bot JWT token:
  Settings -> Bots -> ingestion-bot -> Copy Token

Then set it in this project .env:
  OPENMETADATA_HOST=http://localhost:8585
  OPENMETADATA_JWT_TOKEN=<copied-token>

EOF
